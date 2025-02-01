import pandas as pd

from cleansing import remove_nulls, extract_date


def test_remove_nulls():
    #read test_data2.csv with null record
    df = pd.read_csv('test_data/raw_data/test_data2.csv')
    #remove null records
    result_df = remove_nulls(df)
    #read test data matching test_data2.csv without that null record
    expected_df = pd.read_csv('test_data/cleansed_data/test_data_no_missing_values.csv')

    pd.testing.assert_frame_equal(result_df, expected_df)

def test_extract_date_valid():
    assert extract_date('2024-04-01 00:00:00') == '2024-04-01'
    assert extract_date('2022-03-02 11:22:00') == '2022-03-02'
    assert extract_date('2023-12-31 22:55:59') == '2023-12-31'