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

def load_data(df: object, postgre_table: object, postgre_schema: object) -> object:
    """
    Load transformed data into respective PostgreSQL Table
    :param cur: PostgreSQL cursor object
    :return: cursor object
    """
    # Get column names and placeholders
    columns = ', '.join(df.columns)
    placeholders = ', '.join(['%s' for _ in range(len(df.columns))])

    # Create the INSERT INTO query with placeholders
    insert_query = f"INSERT INTO {postgre_schema}.{postgre_table} ({columns}) VALUES ({placeholders});"

    # Convert pandas DataFrame to a list of tuples (suitable for parameterization)
    data_list = [tuple(row) for row in df.itertuples(index=False, name=None)]

    print(f"Columns: {df.columns}")
    print(f"Placeholders: {placeholders}")
    print(f"Insert Query: {insert_query}")
    print(f"Data List: {data_list}")

    try:
        # Execute the parameterized INSERT INTO query using the data list
        cur.executemany(insert_query, data_list)

        # Commit all changes to the database
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")

# ... (remaining code)
