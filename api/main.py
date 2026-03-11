"""FastAPI entrypoint for the public BioCredits API."""
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from api.routes_v1 import router as v1_router


app = FastAPI(
    title="BioCredits Public API",
    version="1.0.0",
    description="Partner-facing API for environmental credit calculation using approved parameters only.",
    docs_url="/reference",
    redoc_url="/redoc",
)

app.include_router(v1_router)
app.mount("/docs/static", StaticFiles(directory="api/static"), name="docs-static")
templates = Jinja2Templates(directory="api/templates")


@app.get("/", response_class=HTMLResponse, tags=["Documentation"])
def root_docs(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="docs_guide.html",
        context={"title": "BioCredits API Docs"},
    )


@app.get("/docs/guide", response_class=HTMLResponse, tags=["Documentation"])
def docs_guide(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="docs_guide.html",
        context={"title": "BioCredits API Docs"},
    )


@app.get("/dashboard", response_class=HTMLResponse, tags=["Documentation"])
def partner_dashboard(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="partner_dashboard.html",
        context={"title": "BioCredits Partner Dashboard"},
    )

