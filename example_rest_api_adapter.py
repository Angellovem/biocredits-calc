"""
Example REST API Data Adapter - Shows how to implement a generic REST API source
This demonstrates how you could connect to any REST API instead of Airtable.
"""
import os
import requests
import shutil
from typing import Dict, List, Optional, Any
import pandas as pd
import geopandas as gpd
import numpy as np

from data_adapter import ConfigurableAdapter


class RestAPIAdapter(ConfigurableAdapter):
    """
    Generic REST API implementation of the DataAdapter interface.
    This is an EXAMPLE to show how you would implement a REST API data source.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize REST API adapter.
        
        Args:
            config: Configuration dictionary with API endpoints and credentials
        """
        super().__init__(config)
        self.base_url = self.get_config_value('API', 'BASE_URL')
        self.api_key = self.get_config_value('API', 'API_KEY')
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
    
    def _make_request(self, endpoint: str, method: str = 'GET', 
                     data: Optional[Dict] = None) -> Any:
        """
        Make a request to the REST API.
        
        Args:
            endpoint: API endpoint path
            method: HTTP method (GET, POST, PUT, DELETE)
            data: Request data for POST/PUT
            
        Returns:
            Response JSON data
        """
        url = f"{self.base_url}/{endpoint}"
        
        if method == 'GET':
            response = requests.get(url, headers=self.headers)
        elif method == 'POST':
            response = requests.post(url, headers=self.headers, json=data)
        elif method == 'PUT':
            response = requests.put(url, headers=self.headers, json=data)
        elif method == 'DELETE':
            response = requests.delete(url, headers=self.headers)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
        
        response.raise_for_status()
        return response.json()
    
    def download_land_data(self, save_directory: str = 'KML/', 
                          save_shp_directory: str = 'SHPoriginal/') -> pd.DataFrame:
        """
        Download land plot data from REST API.
        
        Example API call:
        GET /api/v1/land-plots
        Response: [
            {
                "plot_id": "001",
                "POD": "POD-001",
                "project_biodiversity": "PROJECT-A",
                "area_certifier": 100.5,
                "kml_url": "https://...",
                "shapefile_url": "https://..."
            }
        ]
        """
        # Create directories
        for directory in [save_directory, save_shp_directory]:
            if not os.path.exists(directory):
                os.makedirs(directory)
            else:
                shutil.rmtree(directory)
                os.makedirs(directory)
        
        # Fetch land plots from API
        # data = self._make_request('land-plots')
        
        # Download files
        # for plot in data:
        #     plot_id = plot['plot_id']
        #     if plot.get('kml_url'):
        #         kml_response = requests.get(plot['kml_url'])
        #         with open(f"{save_directory}/{plot_id}.kml", 'wb') as f:
        #             f.write(kml_response.content)
        #     
        #     if plot.get('shapefile_url'):
        #         shp_response = requests.get(plot['shapefile_url'])
        #         # Extract shapefile...
        
        # Return metadata
        # df = pd.DataFrame(data)
        # return df[['plot_id', 'POD', 'project_biodiversity', 'area_certifier']]
        
        raise NotImplementedError("RestAPIAdapter is a template. Implement API calls here.")
    
    def download_observations(self) -> pd.DataFrame:
        """
        Download biodiversity observations from REST API.
        
        Example API call:
        GET /api/v1/observations?years=10&has_score=true
        Response: [
            {
                "eco_id": "ECO-001",
                "eco_date": "2024-01-15",
                "name_common": "Jaguar",
                "name_latin": "Panthera onca",
                "radius": 5.0,
                "score": 0.95,
                "lat": 0.7,
                "long": -77.0,
                "iNaturalist": "12345"
            }
        ]
        """
        # data = self._make_request('observations?years=10&has_score=true')
        # df = pd.DataFrame(data)
        # df['eco_date'] = pd.to_datetime(df['eco_date'])
        # return df
        
        raise NotImplementedError("RestAPIAdapter is a template. Implement API calls here.")
    
    def upload_results(self, data: pd.DataFrame, table_name: str, 
                      insert_geo: bool = False, delete_all: bool = False) -> None:
        """
        Upload calculation results to REST API.
        
        Example API call:
        POST /api/v1/results/{table_name}
        Body: [
            {"field1": "value1", "field2": "value2"},
            ...
        ]
        """
        # Prepare data
        # if 'geometry' in data.columns and not insert_geo:
        #     data = data.drop(columns=['geometry'])
        # elif insert_geo and 'geometry' in data.columns:
        #     data['geometry'] = data['geometry'].apply(lambda x: x.wkt)
        
        # Convert to records
        # records = data.to_dict('records')
        
        # Clear table if requested
        # if delete_all:
        #     self._make_request(f'results/{table_name}', method='DELETE')
        
        # Upload in batches
        # batch_size = 100
        # for i in range(0, len(records), batch_size):
        #     batch = records[i:i+batch_size]
        #     self._make_request(f'results/{table_name}', method='POST', data={'records': batch})
        
        raise NotImplementedError("RestAPIAdapter is a template. Implement API calls here.")
    
    def log_entry(self, event: str, info: str) -> None:
        """
        Log an entry via REST API.
        
        Example API call:
        POST /api/v1/logs
        Body: {"event": "Start time", "info": "2024-01-15 10:30:00"}
        """
        # self._make_request('logs', method='POST', data={'event': event, 'info': info})
        print(f"LOG [{event}]: {info}")  # Fallback to console logging
    
    def clear_tables(self, table_names: List[str]) -> None:
        """
        Clear tables via REST API.
        
        Example API call:
        DELETE /api/v1/results/{table_name}
        """
        # for table_name in table_names:
        #     self._make_request(f'results/{table_name}', method='DELETE')
        
        raise NotImplementedError("RestAPIAdapter is a template. Implement API calls here.")
    
    def get_area_certifier(self) -> pd.DataFrame:
        """
        Get area certifier data from REST API.
        
        Example API call:
        GET /api/v1/land-plots/area-certifier
        """
        # data = self._make_request('land-plots/area-certifier')
        # df = pd.DataFrame(data)
        # return df.fillna(0)
        
        raise NotImplementedError("RestAPIAdapter is a template. Implement API calls here.")
    
    def fetch_linked_record_name(self, record_id: str, field_name: str) -> Optional[str]:
        """
        Fetch linked record from REST API.
        
        Example API call:
        GET /api/v1/records/{record_id}?field={field_name}
        """
        # data = self._make_request(f'records/{record_id}?field={field_name}')
        # return data.get(field_name)
        
        raise NotImplementedError("RestAPIAdapter is a template. Implement API calls here.")


# Example usage in your config.json:
# {
#   "API": {
#     "BASE_URL": "https://api.yourservice.com/v1",
#     "API_KEY": "your_api_key_here"
#   }
# }

