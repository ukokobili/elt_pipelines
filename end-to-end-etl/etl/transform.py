import pandas as pd

# transform data
def transform_data(df: object) -> object:
    """
    Simple Transformation Function in Python with Error Handling
    :param df: pandas dataframe, extracted data
    :output: pandas dataframe, transformed data
    """

    # drop duplicate rows
    df = df.drop_duplicates()

    # replace missing values in numeric columns with the mean
    numeric_columns = df.select_dtypes(include=['number']).columns
    df[numeric_columns] = df[numeric_columns].apply(lambda x: x.fillna(x.mean()))

    # replace missing values in categorical columns with the mode
    df = df.apply(lambda x: x.fillna(x.mode().iloc[0]) if not x.mode().empty else x)

    # convert columns to appropriate data types
    try:
        df['crash_date'] = pd.to_datetime(df['crash_date'], format='%m/%d/%Y')
    except:
        pass

    try:
        df['posted_speed_limit'] = df['posted_speed_limit'].astype('int32')
    except:
        pass

    # # merge the three dataframes into a single dataframe
    # merge_01_df = pd.merge(df, df2, on='CRASH_RECORD_ID')
    # all_data_df = pd.merge(merge_01_df, df3, on='CRASH_RECORD_ID')
    
    # # drop unnecessary columns
    # df = df[['person_id', 'crash_record_id', 'crash_date', 'person_type', 'vehicle_id', 
    #         'sex', 'age', 'crash_unit_id', 'make', 'model', 'vehicle_year', 
    #         'vehicle_type', 'posted_speed_limit', 'crash_type', 'num_units', 'injuries_total']]
    # Define expected columns
    expected_columns = ['person_id', 'crash_record_id', 'crash_date', 'person_type', 'vehicle_id', 
                        'sex', 'age', 'crash_unit_id', 'make', 'model', 'vehicle_year', 
                        'vehicle_type', 'posted_speed_limit', 'crash_type', 'num_units', 'injuries_total']

    # Check if any expected columns are missing
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        print(f"Warning: The following columns are missing in the DataFrame: {missing_columns}")
        # Optional: Handle missing columns, e.g., by returning None or a modified DataFrame

    # Select only columns that exist in the DataFrame
    existing_columns = [col for col in expected_columns if col in df.columns]
    df = df[existing_columns]
    
    return df
