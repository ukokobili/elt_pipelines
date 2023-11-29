# import relevant modules
import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# establish connection to the PostgreSQL database
conn = psycopg2.connect(
    database=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD'),
    host=os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT')
)

# create a cursor object for running SQL queries
cur = conn.cursor()
print('successful creation of cursor object.')

def load_data(df: object, postgre_schema: object, postgre_table: object, insert_query: object) -> object:
    """
    Load transformed data into respective PostgreSQL Table
    :param df: DataFrame containing the data to load
    :param postgre_schema: PostgreSQL schema name
    :param postgre_table: PostgreSQL table name
    :param insert_query: Insert query template from config
    :return: None
    """
    full_table_name = f"{postgre_schema}.{postgre_table}"
    insert_query_full = f"INSERT INTO {full_table_name} {insert_query}"  # Construct the full query

    for index, row in df.iterrows():
        # Construct a tuple of values to insert
        insert_values = tuple(row[col] for col in row.index)

        # Debugging: Uncomment the next two lines to check the query and values
        print("Executing SQL:", insert_query_full)
        print("With values:", insert_values)

        cur.execute(insert_query_full, insert_values)

    conn.commit()

def close_conn():
    """
    Closing PostgreSQL connection
    """
    cur.close()
    conn.close()
    print('successful closing of cursor object.')

# Example usage
# load_data(df=your_dataframe, postgre_schema='your_schema', postgre_table='your_table', insert_query='your_insert_query')
# close_conn()
