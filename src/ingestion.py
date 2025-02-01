import os
import pandas as pd


def read_all_files(filepath: str):
    """
    Reads all CSV files in filepath to build a dataframe

    Returns:
        pd.DataFrame: dataframe containing all data in filepath
    """
    data_frames = []
    # Iterate over all files in the os directory
    for filename in os.listdir(filepath):
        if filename.endswith('.csv'):
            file_path = os.path.join(filepath, filename)
            df = pd.read_csv(file_path)
            data_frames.append(df)

    return pd.concat(data_frames, ignore_index=True)