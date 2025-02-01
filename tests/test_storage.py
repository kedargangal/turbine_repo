import pytest
import pandas as pd
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String
from storage import create_database,save_data

def test_create_database():
    # Create a database instance
    engine = create_engine('sqlite:///test_connection')

    # Test that the engine can connect to the database
    try:
        with engine.connect() as connection:
            assert connection is not None
    except OperationalError:
        pytest.fail("Database connection failed.")


def test_save_data():
    engine = create_database('test_turbine_database')

    df = pd.DataFrame({
        'column1': [1, 2],
        'column2': ['A', 'B']
    })

    test_table = Table(
        'test', MetaData(),
        Column('column1', Integer),
        Column('column2', String)
    )

    # save test df to test_table
    save_data(df, engine, 'test_table',test_table)

    # query SQL
    result_df = pd.read_sql('SELECT * FROM test_table', con=engine)

    # Assert that the result matches the original DataFrame
    pd.testing.assert_frame_equal(result_df, df)