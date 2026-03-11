"""API key authentication helpers for the public BioCredits API."""
import hashlib
import json
import os
from typing import Dict, List

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from api.schemas import APIKeyPrincipal


bearer = HTTPBearer(auto_error=False)


def _build_key_store() -> Dict[str, APIKeyPrincipal]:
    """
    Build key store from BIOCREDITS_API_KEYS environment variable.

    Expected JSON:
    [
      {"key":"sk_live_xxx","partner_id":"partner_abc","scopes":["calc:write","calc:read"]}
    ]
    """
    raw = os.getenv("BIOCREDITS_API_KEYS")
    if not raw:
        return {}

    try:
        entries = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("BIOCREDITS_API_KEYS is not valid JSON") from exc

    store: Dict[str, APIKeyPrincipal] = {}
    for entry in entries:
        key = entry.get("key")
        partner_id = entry.get("partner_id")
        scopes = entry.get("scopes") or []
        if not key or not partner_id:
            continue
        hashed = hashlib.sha256(key.encode("utf-8")).hexdigest()
        store[hashed] = APIKeyPrincipal(partner_id=partner_id, scopes=list(scopes))
    return store


KEY_STORE = _build_key_store()


def require_api_key(
    credentials: HTTPAuthorizationCredentials = Depends(bearer),
) -> APIKeyPrincipal:
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(status_code=401, detail="Missing API key")

    key_hash = hashlib.sha256(credentials.credentials.encode("utf-8")).hexdigest()
    principal = KEY_STORE.get(key_hash)
    if principal is None:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return principal


def require_scope(principal: APIKeyPrincipal, expected_scope: str) -> None:
    if expected_scope not in principal.scopes:
        raise HTTPException(status_code=403, detail=f"Missing required scope: {expected_scope}")


def list_public_key_scopes() -> List[str]:
    all_scopes: set[str] = set()
    for principal in KEY_STORE.values():
        all_scopes.update(principal.scopes)
    return sorted(all_scopes)


