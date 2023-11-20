# import relevant modules
import duckdb as db

# establish connection to the DuckDB database
conn = db.connect('database_name')

# create a cursor object for running SQL queries
cur = conn.cursor()
print('successful creation of cursor object.')


# suggested continued learning: this function can be modified to be fully dynamic
def load_data(df: object, duckdb_table: object, duckdb_schema: object) -> object:
    """
    Load transformed data into respective DuckDB Table
    :param cur: duckdb cursor object
    :return: cursor object
    """
    insert_query = f"INSERT INTO {duckdb_table} {duckdb_schema};"

    # insert transformed data into the DuckDB table
    # TODO: REFACTOR TO MAKE SENSE - VERY SLOW / POOR USE OF CPUs
    for index, row in df.iterrows():

        if duckdb_table == 'chicago_dmv.crash':
            insert_values = (row['CRASH_UNIT_ID'],
                              row['CRASH_ID'],
                              row['PERSON_ID'],
                              row['VEHICLE_ID'],
                              row['NUM_UNITS'],
                              row['TOTAL_INJURIES'])

        elif duckdb_table == 'chicago_dmv.vehicle':
            insert_values = (row['CRASH_UNIT_ID'],
                              row['CRASH_ID'],
                              row['CRASH_DATE'],
                              row['VEHICLE_ID'],
                              row['VEHICLE_MAKE'],
                              row['VEHICLE_MODEL'],
                              row['VEHICLE_YEAR'],
                              row['VEHICLE_TYPE'])

        elif duckdb_table == 'chicago_dmv.person':
            insert_values = (row['PERSON_ID'],
                              row['CRASH_ID'],
                              row['CRASH_DATE'],
                              row['PERSON_TYPE'],
                              row['VEHICLE_ID'],
                              row['PERSON_SEX'],
                              row['PERSON_AGE'])

        else:
            raise ValueError(f'DuckDB Data Table {duckdb_table} does not exist in this pipeline.')

        # Insert data int
        cur.execute(insert_query, insert_values)

    # Commit all changes to the database
    conn.commit()

def close_conn(cur):
    """
    Closing DuckDB connection
    :param cur: duckdb cursor object
    :return: none
    """

    # Close the cursor and database connection
    cur.close()
    conn.close()
    print('successful closing of cursor object.')