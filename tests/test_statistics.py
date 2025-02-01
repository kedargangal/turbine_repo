import pandas as pd
from transformation import statistics_data

def test_statistics():
    # Sample data test_statistics.csv

    df = pd.read_csv('test_data/stats_anomaly_data/test_statistics.csv')
    result_df = statistics_data(df)
    # Expected data for verification
    expected_data = {
        'turbine_id': [1, 2],
        'date': ['2022-03-01', '2022-03-02'],
        'avg_power_output': [52.50, 60.00],
        'min_power_output': [50.00, 60.00],
        'max_power_output': [55.00, 60.00]
    }
    expected_df = pd.DataFrame(expected_data)
    pd.testing.assert_frame_equal(result_df, expected_df)