# Turbine Data Processing Pipeline

This project implements a data pipeline for renewable energy company that operates a farm of wind turbines. The turbines generate power based on wind speed and direction, and their output is measured in megawatts (MW).
The pipeline processes (Raw data -> cleansed data -> Statistics data -> Anomaly data).Each dataset will be stored in its individual table. It will remove missing values, detect anomalies in power output, and provide statistical analysis of power output. 

# Project Structure
```
turbine-data/
├── src/                         # Source code
│   ├── ingestion.py             # Data ingestion (reading files))
│   ├── cleansing.py             # Data cleansing (remove missing values)
│   ├── transformation.py        # Statistical analysis and anomaly detection
│   ├── storage.py               # Database creation & operations
│   ├── tables.py                # Raw data table -> cleansed data table-> Statistics data table-> Anomaly data table
│   └── turbine_pipeline.py      # runs the pipeline from start to end 
├── tests/                       # Unit tests
├── data/                        # Raw data location 

```
# Design approach

- Pipeline moves with processing Raw data -> cleansed data -> Statistics data -> Anomaly data.At each state data is stored in an individual table.
- Records with missing values will be dropped .
- Tables will be overwritten everytime.
- Pytest to be used as the testing suite.
- Logging and exception handling implemented.

# Testing
- All functions used in the pipeline are tested using the pytest framework.