import pandas as pd
from transformation import anomaly_detection

def test_anomaly_detection():
    # Sample data test_anomaly_detection.csv

    df = pd.read_csv('test_data/stats_anomaly_data/test_anomaly_detection.csv')

    # Expected data
    expected_data = {
        'turbine_id': [1, 1, 1, 1, 1, 1, 1, 1, 2, 2],
        'date': ['2022-03-01', '2022-03-01', '2022-03-01', '2022-03-01', '2022-03-01', '2022-03-01', '2022-03-01',
                 '2022-03-01', '2022-03-01', '2022-03-01'],
        'power_output': [2, 1, 2, 1, 1, 1, 1, 7, 55, 77],
        'avg_power_output': [2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 66.0, 66.0],
        'std_deviation_power_output': [2.07, 2.07, 2.07, 2.07, 2.07, 2.07, 2.07, 2.07, 15.56, 15.56],
        'anomaly': [False, False, False, False, False, False, False, True, False, False]
    }
    expected_df = pd.DataFrame(expected_data)

    # Run the function
    result_df = anomaly_detection(df)
    # Assert that the result DataFrame matches the expected DataFrame
    pd.testing.assert_frame_equal(result_df, expected_df)