import pandas as pd


def statistics_data(df: pd.DataFrame):
    """
    Aggregates data for each turbine_id and date over power_output calculating min,max and average

    Parameters:
        df (pd.DataFrame): The cleansed DataFrame to generate statistics.


    Returns:
        pd.DataFrame: A DataFrame with all statistics data (min,max and average power_output)
    """

    column_names_rename = {'mean': 'avg_power_output', 'min': 'min_power_output', 'max': 'max_power_output'}
    # calculate  min & max & average power_output for turbine_id & date
    stats_df = df.groupby(['turbine_id', 'date'])['power_output'].agg(['mean', 'min', 'max']).round(2).rename(
        columns=column_names_rename).reset_index()
    # Multiindex dataframe to single index flat structure
    stats_df.columns = list(map(''.join, stats_df.columns.values))
    return stats_df


def anomaly_detection(df: pd.DataFrame):
    """
    group data by turbine_id and  date, taking the std deviation & mean of each grouping and checking anomaly

    Parameters:
        df (pd.DataFrame): The DataFrame to clean.

    Returns:
        pd.DataFrame: A DataFrame with anomaly flag for each turbine
    """
    # Grouping by turbine_id and  date to  power output
    grouped = df.groupby(['turbine_id', 'date'])['power_output']

    # calculating mean and std dev
    df['avg_power_output'] = round(grouped.transform('mean'), 2)
    df['std_deviation_power_output'] = round(grouped.transform('std'), 2)

    # Apply boolean anomaly flag to each row
    df['anomaly'] = df.apply(
        lambda x: abs(x['power_output'] - x['avg_power_output']) > 2 * x['std_deviation_power_output'], axis=1
    )
    df_anomaly_final = df[
        ['turbine_id', 'date', 'power_output', 'avg_power_output', 'std_deviation_power_output', 'anomaly']]
    return df_anomaly_final
