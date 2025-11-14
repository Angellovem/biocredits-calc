"""
Data Adapter Interface - Abstract base class for data operations
This allows decoupling from specific data sources (Airtable, databases, APIs, etc.)
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
import pandas as pd
import geopandas as gpd


class DataAdapter(ABC):
    """
    Abstract base class defining the interface for data operations.
    Implement this class to support different data sources (Airtable, PostgreSQL, etc.)
    """
    
    @abstractmethod
    def download_land_data(self, save_directory: str = 'KML/', 
                          save_shp_directory: str = 'SHPoriginal/') -> pd.DataFrame:
        """
        Download land plot data (KML files, shapefiles, and metadata).
        
        Args:
            save_directory: Directory to save KML files
            save_shp_directory: Directory to save shapefiles
            
        Returns:
            DataFrame with land metadata (plot_id, POD, project_biodiversity, area_certifier)
        """
        pass
    
    @abstractmethod
    def download_observations(self) -> pd.DataFrame:
        """
        Download biodiversity observations data.
        
        Returns:
            DataFrame with observations (eco_id, eco_date, name_common, name_latin, 
                                        radius, score, lat, long, iNaturalist)
        """
        pass
    
    @abstractmethod
    def upload_results(self, data: pd.DataFrame, table_name: str, 
                      insert_geo: bool = False, delete_all: bool = False) -> None:
        """
        Upload calculation results to the data destination.
        
        Args:
            data: DataFrame or GeoDataFrame with results
            table_name: Name of the destination table
            insert_geo: Whether to include geometry column
            delete_all: Whether to delete existing records first
        """
        pass
    
    @abstractmethod
    def log_entry(self, event: str, info: str) -> None:
        """
        Log an event/information entry.
        
        Args:
            event: Event name/type
            info: Event information/details
        """
        pass
    
    @abstractmethod
    def clear_tables(self, table_names: List[str]) -> None:
        """
        Clear/delete all records from specified tables.
        
        Args:
            table_names: List of table names to clear
        """
        pass
    
    @abstractmethod
    def get_area_certifier(self) -> pd.DataFrame:
        """
        Get area certifier information for land plots.
        
        Returns:
            DataFrame with plot_id and area_certifier columns
        """
        pass
    
    @abstractmethod
    def fetch_linked_record_name(self, record_id: str, field_name: str) -> Optional[str]:
        """
        Fetch the name of a linked/related record (for relational data).
        
        Args:
            record_id: ID of the linked record
            field_name: Field name to fetch from the linked record
            
        Returns:
            Value of the specified field, or None if not found
        """
        pass


class ConfigurableAdapter(DataAdapter):
    """
    Base class for adapters that use configuration.
    Subclasses should implement the actual data operations.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize adapter with configuration.
        
        Args:
            config: Configuration dictionary. If None, loads from config.json
        """
        if config is None:
            import json
            with open('config.json', 'r') as f:
                config = json.load(f)
        self.config = config
    
    def get_config_value(self, *keys: str, default: Any = None) -> Any:
        """
        Get a nested configuration value.
        
        Args:
            keys: Sequence of keys to traverse the config dict
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        value = self.config
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key, default)
            else:
                return default
        return value

