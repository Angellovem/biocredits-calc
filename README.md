# BioCredits Calc

This repository contains the necessary files for calculating BioCredits. The codebase has been refactored with an **adapter pattern** to decouple data operations from calculation logic, making it easy to switch between different data sources (Airtable, PostgreSQL, MongoDB, REST APIs, etc.).

## Architecture

The code now uses a **DataAdapter interface** to abstract data operations:

```
credits_pipeline.py → calc_utils.py → DataAdapter → AirtableAdapter (or any other adapter)
```

This design allows you to:
- **Switch data sources easily** - change one line to use a different backend
- **Test without real APIs** - use mock adapters for unit testing
- **Maintain clean code** - data access separated from business logic
- **Stay flexible** - add new data sources without changing calculation code

## Quick Start

### Using with Airtable (Current Setup)

1. **Configure your credentials:**
   ```bash
   cp config.json.template config.json
   # Edit config.json with your Airtable credentials
   ```

2. **Run the pipeline:**
   ```bash
   python credits_pipeline.py
   ```

### Switching to a Different Data Source

To use PostgreSQL, MongoDB, or another data source:

1. **Create your adapter** (see `example_postgres_adapter.py` or `example_rest_api_adapter.py`)
2. **Update `credits_pipeline.py`:**
   ```python
   # Change from:
   from airtable_adapter import AirtableAdapter
   adapter = AirtableAdapter()
   
   # To:
   from postgres_adapter import PostgresAdapter
   adapter = PostgresAdapter()
   ```

That's it! The entire pipeline now uses your new data source.

## Detailed Setup

### Prerequisites

Ensure you have [Anaconda](https://www.anaconda.com/products/individual) or [Miniconda](https://docs.conda.io/en/latest/miniconda.html) installed on your system. These tools provide an easy way to manage environments and packages for your projects.

### Setting Up Your Environment
```
sudo apt-get update
sudo apt-get install git
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod +x Miniconda3-latest-Linux-x86_64.sh
./Miniconda3-latest-Linux-x86_64.sh
~/miniconda3/bin/conda init
source ~/.bashrc
sudo apt-get install gdal-bin
sudo apt install ffmpeg
```

1. **Clone the Repository**

   First, clone this repository to your local machine using Git.

   ```bash
   git clone https://github.com/savimbo/biocredits-calc.git
   cd biocredits-calc
   ```
   
### Setting Up Your Environment


2. **Create a Conda Environment**

Create a new Conda environment named `biocredits_env` with Python 3.10. Replace `biocredits_env` with a name of your choice if desired.
```bash
conda create --name biocredits_env python=3.10
```

3. **Activate the Environment**

Activate the newly created environment.
```bash
conda activate biocredits_env
```

4. **Install Requirements**

Install the required packages listed in `requirements.txt`.
```bash
pip install -r requirements.txt
```

### Running the Notebook

With the environment set up and activated, you can now run the Jupyter notebook.

1. **Start Jupyter Notebook**
   ```bash
   jupyter notebook
   ```

2. **Open `testing_notebook.ipynb`**

Navigate through the Jupyter Notebook interface in your browser to the location of the cloned repository. Open the `testing_notebook.ipynb` notebook.

3. **Run the Notebook**


Execute the cells in the notebook to perform the calculations or analyses contained within.

## Adapter Pattern Overview

### Core Components

| File | Purpose |
|------|---------|
| `data_adapter.py` | Abstract interface defining all data operations |
| `airtable_adapter.py` | Airtable-specific implementation |
| `calc_utils.py` | Calculation utilities (now accepts adapter parameter) |
| `credits_pipeline.py` | Main pipeline orchestrator |

### Available Adapters

- **AirtableAdapter** (`airtable_adapter.py`) - Ready to use with your Airtable setup
- **PostgresAdapter** (`example_postgres_adapter.py`) - Example template for PostgreSQL
- **RestAPIAdapter** (`example_rest_api_adapter.py`) - Example template for generic REST APIs

### Creating a Custom Adapter

Implement the `DataAdapter` interface to support any data source:

```python
from data_adapter import ConfigurableAdapter

class MyCustomAdapter(ConfigurableAdapter):
    def download_land_data(self, save_directory, save_shp_directory):
        # Your implementation
        pass
    
    def download_observations(self):
        # Your implementation
        pass
    
    def upload_results(self, data, table_name, insert_geo, delete_all):
        # Your implementation
        pass
    
    def log_entry(self, event, info):
        # Your implementation
        pass
    
    # ... implement other required methods
```

See `example_postgres_adapter.py` and `example_rest_api_adapter.py` for complete examples.

### Function Signatures

Functions that interact with data sources now accept an `adapter` parameter:

```python
# Data operations (require adapter)
download_kml_official(adapter)
download_observations(adapter)
insert_log_entry(adapter, event, info)
insert_gdf_to_airtable(adapter, gdf, table, ...)
clear_biocredits_tables(adapter, tables)

# Geographic/calculation functions (no adapter needed)
kml_to_shp(source_dir, dest_dir, ...)
load_shp(directory)
normalize_shps(gdfs)
observations_to_circles(records, ...)
daily_score_union(obs_expanded)
# ... and many more
```

## Configuration

Create a `config.json` file from the template:

```bash
cp config.json.template config.json
```

Edit with your credentials:

```json
{
  "PERSONAL_ACCESS_TOKEN": "your_airtable_token",
  "KML_TABLE": {
    "BASE_ID": "your_base_id",
    "TABLE_NAME": "your_table_name",
    ...
  },
  ...
}
```

For other data sources, add their configuration to the same file:

```json
{
  "POSTGRES": {
    "HOST": "localhost",
    "DATABASE": "biocredits",
    "USER": "username",
    "PASSWORD": "password"
  }
}
```

## Testing

Create a mock adapter for testing without real API calls:

```python
from data_adapter import DataAdapter
import pandas as pd

class MockAdapter(DataAdapter):
    def download_land_data(self, *args, **kwargs):
        return pd.DataFrame({
            'plot_id': ['001', '002'],
            'POD': ['POD-A', 'POD-B'],
            'project_biodiversity': ['PROJ-1', 'PROJ-1'],
            'area_certifier': [100, 150]
        })
    
    # ... implement other methods with test data

adapter = MockAdapter()
run_biocredits_pipeline(adapter)
```

## Contributing

Feel free to fork the repository and submit pull requests with enhancements or fixes. When contributing:

- Keep data access logic in adapters
- Keep calculation logic in `calc_utils.py`
- Update tests to use mock adapters
- Document any new adapter implementations

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.



