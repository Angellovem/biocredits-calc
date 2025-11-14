"""
Airtable Data Adapter - Implements DataAdapter for Airtable API
"""
import os
import requests
import shutil
import time
import zipfile
from typing import Dict, List, Optional, Any
import pandas as pd
import geopandas as gpd
import numpy as np

from data_adapter import ConfigurableAdapter


class AirtableAdapter(ConfigurableAdapter):
    """
    Airtable-specific implementation of the DataAdapter interface.
    Handles all interactions with Airtable API.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize Airtable adapter.
        
        Args:
            config: Configuration dictionary with Airtable credentials and endpoints
        """
        super().__init__(config)
        self._record_cache: Dict[str, Dict[str, Any]] = {}
    
    def _get_headers(self, token_key: str = 'PERSONAL_ACCESS_TOKEN') -> Dict[str, str]:
        """Get Airtable API headers with authentication."""
        token = self.get_config_value(token_key)
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    def _fetch_all_records(self, base_id: str, table_name: str, 
                          view_id: Optional[str] = None,
                          token_key: str = 'PERSONAL_ACCESS_TOKEN') -> List[Dict[str, Any]]:
        """
        Fetch all records from an Airtable table (handles pagination).
        
        Args:
            base_id: Airtable base ID
            table_name: Table name or ID
            view_id: Optional view ID to filter records
            token_key: Config key for the access token
            
        Returns:
            List of all records
        """
        endpoint = f"https://api.airtable.com/v0/{base_id}/{table_name}"
        headers = self._get_headers(token_key)
        
        all_records = []
        offset = None
        
        while True:
            params = {}
            if view_id:
                params['view'] = view_id
            if offset:
                params['offset'] = offset
            
            response = requests.get(endpoint, headers=headers, params=params)
            response.raise_for_status()
            response_json = response.json()
            
            records = response_json.get('records', [])
            all_records.extend(records)
            
            offset = response_json.get('offset')
            if not offset:
                break
        
        return all_records
    
    def download_land_data(self, save_directory: str = 'KML/', 
                          save_shp_directory: str = 'SHPoriginal/') -> pd.DataFrame:
        """
        Download KML files and shapefiles from Airtable, and additional metadata.
        Only process rows that have either KML or shapefile data.
        """
        # Create directories if they don't exist
        for directory in [save_directory, save_shp_directory]:
            if not os.path.exists(directory):
                os.makedirs(directory)
            else:
                shutil.rmtree(directory)
                os.makedirs(directory)
        
        # Get configuration
        base_id = self.get_config_value('KML_TABLE', 'BASE_ID')
        table_name = self.get_config_value('KML_TABLE', 'TABLE_NAME')
        view_id = self.get_config_value('KML_TABLE', 'VIEW_ID')
        field = self.get_config_value('KML_TABLE', 'FIELD')
        
        # Fetch all records
        all_records = self._fetch_all_records(base_id, table_name, view_id)
        
        # Create caches for POD and project_biodiversity lookups
        pod_cache = {}
        proj_bio_cache = {}
        
        # Process records
        metadata = []
        good_plots = 0
        shp_downloaded = 0
        total_records = 0
        
        for record in all_records:
            fields = record['fields']
            kml_field = fields.get(field)
            shapefile = fields.get('shapefile_polygon')
            
            # Skip if neither KML nor shapefile is available
            if not kml_field and not shapefile:
                continue
            
            total_records += 1
            plot_id = str(fields.get('plot_id'))
            plot_id = f"{plot_id:0>3}"
            
            # Download KML if available
            if kml_field:
                url = kml_field[0]['url']
                save_path = os.path.join(save_directory, plot_id + '.kml')
                
                with requests.get(url, stream=True) as file_response:
                    with open(save_path, 'wb') as file:
                        for chunk in file_response.iter_content(chunk_size=8192):
                            file.write(chunk)
                good_plots += 1
                print(f"Downloaded KML for plot_id {plot_id}")
            
            # Download and extract shapefile if available
            if shapefile:
                url = shapefile[0]['url']
                plot_shp_dir = os.path.join(save_shp_directory, plot_id)
                if not os.path.exists(plot_shp_dir):
                    os.makedirs(plot_shp_dir)
                
                zip_path = os.path.join(plot_shp_dir, f"{plot_id}.zip")
                with requests.get(url, stream=True) as file_response:
                    with open(zip_path, 'wb') as file:
                        for chunk in file_response.iter_content(chunk_size=8192):
                            file.write(chunk)
                
                try:
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(plot_shp_dir)
                    print(f"Downloaded and extracted shapefile for plot_id {plot_id}")
                    os.remove(zip_path)
                    shp_downloaded += 1
                except zipfile.BadZipFile:
                    print(f"Error: Invalid zip file for plot_id {plot_id}")
                    continue
            
            # Fetch actual values for POD and project_biodiversity
            pod_id = fields.get('POD', [''])[0] if isinstance(fields.get('POD'), list) else fields.get('POD', '')
            proj_bio_id = fields.get('project_biodiversity', [''])[0] if isinstance(fields.get('project_biodiversity'), list) else fields.get('project_biodiversity', '')
            
            pod_name = self.fetch_linked_record_name(pod_id, 'CODE', cache=pod_cache, 
                                                     base_id=base_id, table_name=table_name) if pod_id else ''
            proj_bio_name = self.fetch_linked_record_name(proj_bio_id, 'project_id', cache=proj_bio_cache,
                                                          base_id=base_id, table_name=table_name) if proj_bio_id else ''
            
            # Collect metadata with actual values
            metadata.append({
                'plot_id': plot_id.zfill(3),
                'POD': pod_name,
                'project_biodiversity': proj_bio_name,
                'area_certifier': fields.get('area_certifier', 0)
            })
        
        # Save metadata to DataFrame
        metadata_df = pd.DataFrame(metadata)
        metadata_df.to_csv('land_metadata.csv', index=False)
        
        # Log statistics
        self.log_entry('Unique PODs found:', str(metadata_df['POD'].value_counts(dropna=False).to_dict()))
        self.log_entry('Unique Project Biodiversity found:', str(metadata_df['project_biodiversity'].value_counts(dropna=False).to_dict()))
        self.log_entry('Total records with KML or shapefile:', str(total_records))
        self.log_entry('Total KMLs downloaded:', str(good_plots))
        self.log_entry('Total shapefiles downloaded:', str(shp_downloaded))
        
        return metadata_df
    
    def download_observations(self) -> pd.DataFrame:
        """
        Download biodiversity observations from Airtable.
        """
        # Get configuration
        base_id = self.get_config_value('OBS_TABLE', 'BASE_ID')
        table_id = self.get_config_value('OBS_TABLE', 'TABLE_ID')
        view_id = self.get_config_value('OBS_TABLE', 'VIEW_ID')
        
        # Fetch all records
        all_records = self._fetch_all_records(base_id, table_id, view_id)
        
        self.log_entry('Total observations fetched:', str(len(all_records)))
        
        records = pd.DataFrame([r['fields'] for r in all_records])
        
        # Keep records with integrity score
        records = records[[not r is np.nan for r in records["integrity_score"]]]
        self.log_entry('Observations with integrity score:', str(len(records)))
        
        # Transform and create columns
        records['name_latin'] = records['name_latin'].apply(lambda x: x[0] if type(x) == list and len(x) == 1 else str(x))
        records['score'] = records['integrity_score'].apply(lambda x: max(x) if type(x) == list else x)
        records['radius'] = records['calc_radius'].apply(lambda x: round(max(x), 2) if type(x) == list else x)
        
        cache = {}
        records['name_common'] = records['name_common_es'].apply(lambda x: x[0] if type(x) == list and len(x) == 1 else str(x))
        
        # Fetch linked species names
        endpoint = f"https://api.airtable.com/v0/{base_id}/{table_id}"
        headers = self._get_headers()
        records['name_common_'] = records['species_type'].apply(
            lambda x: self._fetch_linked_record_direct(x[0], headers, cache, endpoint, 'species_name_common_es') 
            if type(x) == list and len(x) == 1 else None)
        
        # Filter records
        records = records.query('radius>0')
        self.log_entry('Observations with radius > 0:', str(len(records)))
        records = records.query('eco_long<0')
        self.log_entry('Observations with eco_long < 0:', str(len(records)))
        
        # Rename and keep columns
        records = records.rename(columns={'# ECO': 'eco_id', 'eco_lat': 'lat', 'eco_long': 'long'})
        keep_columns = ['eco_id', 'eco_date', 'name_common', 'name_latin', 'radius', 'score', 'lat', 'long', 'iNaturalist']
        records = records[keep_columns].sort_values(by=['eco_date'])
        records['eco_date'] = pd.to_datetime(records['eco_date'])
        
        # Filter out observations older than 10 years
        records = records[records['eco_date'] >= (pd.Timestamp.now() - pd.DateOffset(years=10))]
        self.log_entry('Observations < 10 years old:', str(len(records)))
        self.log_entry('Observations WITHOUT iNaturalist:', str(records['iNaturalist'].isna().sum()))
        self.log_entry('Observations used:', str(len(records)))
        self.log_entry('Scores seen:', str(list(np.sort(records['score'].unique())[::-1])))
        self.log_entry('Radius seen:', str(list(np.sort(records['radius'].unique())[::-1])))
        
        records.sort_values('eco_date', ascending=False, inplace=True)
        return records
    
    def _fetch_linked_record_direct(self, record_id: str, headers: Dict[str, str], 
                                   cache: Dict[str, Any], endpoint: str, 
                                   field_name: str = 'species_name_common_es') -> Optional[str]:
        """Helper method for fetching linked records with custom headers."""
        if record_id in cache:
            return cache[record_id]
        
        response = requests.get(f"{endpoint}/{record_id}", headers=headers)
        if response.status_code == 200:
            data = response.json()
            value = data['fields'].get(field_name)
            cache[record_id] = value
            return value
        else:
            print(f"Error fetching record {record_id}:", response.text)
            return None
    
    def upload_results(self, data: pd.DataFrame, table_name: str, 
                      insert_geo: bool = False, delete_all: bool = False) -> None:
        """
        Upload results to Airtable.
        """
        gdf = data.copy()
        
        base_id = self.get_config_value('BIOCREDITS-CALC', 'BASE_ID')
        token = self.get_config_value('PAT_BIOCREDITS-CALC')
        
        if insert_geo and 'geometry' in gdf.columns:
            gdf['geometry'] = gdf['geometry'].apply(lambda x: x.wkt)
        elif not insert_geo and 'geometry' in gdf.columns:
            gdf.drop(columns=['geometry'], inplace=True)
        
        for col in gdf.columns:
            if gdf[col].dtype == 'datetime64[ns]':
                gdf[col] = gdf[col].astype(str)
            if gdf[col].dtype == 'O':
                gdf[col] = gdf[col].astype(str)
        
        gdf.fillna('', inplace=True)
        records = gdf.to_dict('records')
        
        api_url = f"https://api.airtable.com/v0/{base_id}/{table_name}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        if delete_all:
            self._delete_all_records(headers, api_url)
        
        # Batch insert
        batch_size = 10
        chunks = [records[i:i + batch_size] for i in range(0, len(records), batch_size)]
        
        for chunk in chunks:
            json_call = {"records": [{"fields": record} for record in chunk]}
            response = requests.post(api_url, headers=headers, json=json_call)
            time.sleep(0.2)
            if response.status_code != 200:
                print("Error:", response.text)
    
    def _delete_all_records(self, headers: Dict[str, str], api_url: str) -> bool:
        """Delete all records from an Airtable table."""
        all_record_ids = []
        
        response = requests.get(api_url, headers=headers)
        if response.status_code != 200:
            print("Error fetching record IDs:", response.text)
            return False
        
        records = response.json().get('records', [])
        all_record_ids.extend([record['id'] for record in records])
        
        # Continue fetching records until we've got them all
        response_json = response.json()
        while 'offset' in response_json:
            offset = response_json.get('offset')
            response = requests.get(f"{api_url}?offset={offset}", headers=headers)
            response_json = response.json()
            records = response_json.get('records', [])
            all_record_ids.extend([record['id'] for record in records])
        
        # Delete the records using their IDs
        for record_id in all_record_ids:
            del_response = requests.delete(f"{api_url}/{record_id}", headers=headers)
            time.sleep(0.2)
            if del_response.status_code != 200:
                print(f"Error deleting record {record_id}:", del_response.text)
        
        return True
    
    def log_entry(self, event: str, info: str) -> None:
        """Log an entry to Airtable Logs table."""
        df = pd.DataFrame({'Event': [event], 'Info': [info]})
        self.upload_results(df, "Logs", insert_geo=False, delete_all=False)
    
    def clear_tables(self, table_names: List[str]) -> None:
        """Clear multiple Airtable tables using webhooks."""
        # Trigger webhooks twice for reliability
        for table in table_names:
            self._trigger_delete_webhook(table)
        for table in table_names:
            self._trigger_delete_webhook(table)
        
        time.sleep(5)
        
        # Verify deletion and retry if needed
        delete_again = []
        base_id = self.get_config_value('BIOCREDITS-CALC', 'BASE_ID')
        token = self.get_config_value('PAT_BIOCREDITS-CALC')
        
        for table_name in table_names:
            endpoint = f"https://api.airtable.com/v0/{base_id}/{table_name}"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            response = requests.get(endpoint, headers=headers)
            response_json = response.json()
            if len(response_json.get('records', [])) > 0:
                delete_again.append(table_name)
        
        if len(delete_again) > 0:
            self.clear_tables(delete_again)
        
        time.sleep(10)
    
    def _trigger_delete_webhook(self, table: str) -> None:
        """Trigger Airtable webhook to delete table contents."""
        url = self.get_config_value("BIOCREDITS-CALC", "DELETE_TABLE_WEBHOOK", table)
        if url:
            headers = {"Content-Type": "application/json"}
            requests.post(url, headers=headers, data="{}")
    
    def get_area_certifier(self) -> pd.DataFrame:
        """Get area certifier data from Airtable."""
        base_id = self.get_config_value('KML_TABLE', 'BASE_ID')
        table_name = self.get_config_value('KML_TABLE', 'TABLE_NAME')
        view_id = self.get_config_value('KML_TABLE', 'VIEW_ID')
        
        all_records = self._fetch_all_records(base_id, table_name, view_id)
        
        area_cert = []
        for record in all_records:
            a = {}
            a['plot_id'] = record['fields'].get('plot_id')
            a['area_certifier'] = record['fields'].get('area_certifier')
            area_cert.append(a)
        
        return pd.DataFrame(area_cert).fillna(0)
    
    def fetch_linked_record_name(self, record_id: str, field_name: str,
                                 cache: Optional[Dict[str, Any]] = None,
                                 base_id: Optional[str] = None,
                                 table_name: Optional[str] = None) -> Optional[str]:
        """
        Fetch the name of a linked record from Airtable.
        
        Args:
            record_id: Record ID to fetch
            field_name: Field name to retrieve
            cache: Optional cache dictionary to avoid duplicate requests
            base_id: Airtable base ID (uses KML_TABLE base if not provided)
            table_name: Table name (uses KML_TABLE if not provided)
        """
        if cache is None:
            cache = self._record_cache
        
        if record_id in cache:
            return cache[record_id]
        
        if base_id is None:
            base_id = self.get_config_value('KML_TABLE', 'BASE_ID')
        if table_name is None:
            table_name = self.get_config_value('KML_TABLE', 'TABLE_NAME')
        
        endpoint = f"https://api.airtable.com/v0/{base_id}/{table_name}"
        headers = self._get_headers()
        
        response = requests.get(f"{endpoint}/{record_id}", headers=headers)
        if response.status_code == 200:
            data = response.json()
            value = data['fields'].get(field_name)
            cache[record_id] = value
            return value
        else:
            print(f"Error fetching record {record_id}:", response.text)
            return None

