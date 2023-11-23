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
    # df = df[['CRASH_UNIT_ID', 'CRASH_ID', 'CRASH_DATE', 'VEHICLE_ID', 'VEHICLE_MAKE', 'VEHICLE_MODEL',
    #          'VEHICLE_YEAR', 'VEHICLE_TYPE', 'PERSON_ID', 'PERSON_TYPE', 'PERSON_SEX', 'PERSON_AGE',
    #          'CRASH_HOUR', 'CRASH_DAY_OF_WEEK', 'CRASH_MONTH', 'DATE_POLICE_NOTIFIED']]
    return df
