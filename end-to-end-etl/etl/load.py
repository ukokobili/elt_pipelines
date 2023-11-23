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
    insert_query = f"INSERT INTO {postgre_table} {postgre_schema};"

    # insert transformed data into PostgreSQL table
    # TODO: REFACTOR TO MAKE SENSE - VERY SLOW / POOR USE OF CPUs
    for index, row in df.iterrows():

        if postgre_table == 'chicago_dmv.crash':
            insert_values = (row['crash_record_id'],
                              row['crash_date'],
                              row['posted_speed_limit'],
                              row['crash_type'],
                              row['num_units'],
                              row['injuries_total'])

        elif postgre_table == 'chicago_dmv.vehicle':
            insert_values = (row['crash_unit_id'],
                              row['crash_record_id'],
                              row['crash_date'],
                              row['vehicle_id'],
                              row['make'],
                              row['model'],
                              row['vehicle_year'],
                              row['vehicle_type'])

        elif postgre_table == 'chicago_dmv.person':
            insert_values = (row['person_id'],
                              row['crash_record_id'],
                              row['crash_date'],
                              row['person_type'],
                              row['vehicle_id'],
                              row['sex'],
                              row['age'])

        else:
            raise ValueError(f'Postgre Data Table {postgre_table} does not exist in this pipeline.')

        # Insert data int
        cur.execute(insert_query, insert_values)

    # Commit all changes to the database
    conn.commit()

def close_conn(cur):
    """
    Closing Postgre connection
    :param cur: posgre cursor object
    :return: none
    """

    # Close the cursor and database connection
    cur.close()
    conn.close()
    print('successful closing of cursor object.')