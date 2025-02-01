
import pandas as pd
from ingestion import read_all_files

def test_read_all_files():
    # READ  CSVs in a test_data directory

    # Call read_all_files
    result_df = read_all_files(filepath='test_data/raw_data')

    # Expected DataFrame is both test_data1.csv and test_data2.csv
    df1 = pd.read_csv('test_data/raw_data/test_data1.csv')
    df2 = pd.read_csv('test_data/raw_data/test_data2.csv')
    expected_df = pd.concat([df1, df2], ignore_index=True)
    # Assert that the result matches the expected DataFrame
    pd.testing.assert_frame_equal(result_df, expected_df)