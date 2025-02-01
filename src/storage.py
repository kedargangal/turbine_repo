import pandas as pd
from sqlalchemy import create_engine,Table
from sqlalchemy.engine import Engine


def create_database(database_name: str):
    """
    Creates a database engine connection to the SQL database.

    Parameters:
        database_name (str): Name of database.

    Returns:
        database_engine: Our SQL database instance.
    """
    database_engine = create_engine(f'sqlite:///{database_name}')

    return database_engine


def save_data(df: pd.DataFrame, database_engine: Engine, table_name: str,db_table :Table):
    """
    save dataframe to sql database

    Parameters:
        df (pd.DataFrame:): dataframe object
        db_name (sqlite object): database name
        table_name: desired table name

    """
    db_table.metadata.create_all(database_engine)

    return df.to_sql(table_name, con=database_engine, if_exists='replace', index=False)