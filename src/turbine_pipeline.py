
import pandas as pd
import logging

import tables
from storage import create_database,save_data
from ingestion import read_all_files
from cleansing import remove_nulls, extract_date
from transformation import anomaly_detection, statistics_data


# logging to console
logging.basicConfig(level=logging.INFO, format='%(asctime)s : %(levelname)s : %(message)s')
logger = logging.getLogger(__name__)



def populate_raw_data(database_engine,filepath):
    try:
        # Read raw data from an os path
        raw_df = read_all_files(filepath)
        logger.info(f"Loaded {len(raw_df)} rows of raw data")
        # save Raw data
        save_data(df=raw_df, database_engine=database_engine, table_name='raw_turbines',db_table = tables.raw_data)
        logger.info("Successfully created raw data")

    except Exception as e:
        logger.error(f"Error in raw data creation: {str(e)}")
        raise e


def populate_cleansed_data(database_engine):
    try:
        # Read raw data
        raw_df = pd.read_sql('SELECT * FROM raw_turbines', con=database_engine)
        logger.info(f"Loaded {len(raw_df)} rows from raw data")

        # Create cleansed data table
        cleansed_df = remove_nulls(raw_df)
        logger.info(f"Removed nulls, {len(cleansed_df)} rows remaining")
        # Add date column from timestamp for later stats_anomaly_data

        cleansed_df.loc[:, 'date'] = cleansed_df['timestamp'].apply(extract_date)

        # save cleansed data
        save_data(df=cleansed_df, database_engine=database_engine, table_name='cleansed_turbines',db_table = tables.cleansed_data)
        logger.info("Successfully created cleansed data")

    except Exception as e:
        logger.error(f"Error in cleansed data creation: {str(e)}")
        raise e


def populate_summary_statistics_data(database_engine):
    try:
        # Read cleansed data
        cleansed_df = pd.read_sql('SELECT distinct turbine_id,power_output,date FROM cleansed_turbines', con=database_engine)

        logger.info(f"Loaded {len(cleansed_df)} rows from cleansed data")

        # save summary statistics data
        statistics_df = statistics_data(cleansed_df)

        logger.info(f"Statistics data, {len(statistics_df)} rows ")
        save_data(df=statistics_df, database_engine=database_engine, table_name='summary_statistics_turbines',db_table = tables.summary_statistics_data)

        logger.info("Successfully created summary statistics data table")

    except Exception as e:
        logger.error(f"Error in summary statistics data creation: {str(e)}")
        raise e

def populate_anomaly_data(database_engine):
    try:
        # Read cleansed data
        cleansed_df = pd.read_sql('SELECT distinct turbine_id,power_output,date FROM cleansed_turbines', con=database_engine)

        logger.info(f"Loaded {len(cleansed_df)} rows from cleansed data")

        # calculate anomaly for each turbine
        anomaly_df = anomaly_detection(cleansed_df)
        logger.info(f"Anomaly data, {len(anomaly_df)} rows ")
        #  save anomaly data
        save_data(df=anomaly_df, database_engine=database_engine, table_name='anomaly_turbines',db_table = tables.anomaly_data)

        logger.info("Successfully created Anomaly data table")

    except Exception as e:
        logger.error(f"Error in Anomaly data creation: {str(e)}")
        raise e

def turbine_processing():
    try:
        logger.info("Starting turbine data pipeline")
        # creating a sql  database
        database_engine = create_database(database_name='turbine.db')

        populate_raw_data(database_engine=database_engine,filepath='../data')
        populate_cleansed_data(database_engine=database_engine)
        populate_summary_statistics_data(database_engine=database_engine)
        populate_anomaly_data(database_engine=database_engine)
        logger.info("Completed turbine data Pipeline Successfully")
    except Exception as e:
        logger.error(f"  Turbine data Pipeline failed: {str(e)}")
        raise e


if __name__ == "__main__":
    turbine_processing()