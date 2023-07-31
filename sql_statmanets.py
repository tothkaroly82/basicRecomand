#%%
import pyodbc
import pandas as pd
import yaml
# %%
def sqlSelectToDataFrame(sqlFile,queryNum):
    '''
    beolvassa a lekerdezest sql file-bol, vegrehajtja az sql-t, az eredmenyt
    betolti egy dataframbe, sqlFile file amibol olvassuk, queryNum a lekerdezes szama, 
    ; valasztjuk el a lekerdezeseket
    '''
    # read credentials from confiq
    with open("config.yml", 'r') as stream:
        try:
            parsedYaml=yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    server = parsedYaml['server']['mdbwh']
    database = parsedYaml['database']['DaxDBWH']
    username = parsedYaml['credentials']['username']
    password = parsedYaml['credentials']['password']

    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    fd = open(sqlFile, 'r')
    query = fd.read()
    query=query.split(';')[queryNum]
    fd.close()  
    df_query = pd.read_sql_query(query,conn)
    return df_query
# %%
def delete_data_from_table(connection_string, destination_table):

    try:
        # Establish a connection to the database
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()

        # delete data from specific table
        cursor.execute(f"delete from {destination_table};")
        print(f" data deleted successfully from {destination_table}")

        connection.commit()
        connection.close()

    except pyodbc.Error as e:
        print("Error:", e)

def create_connection_string(server, db):
    with open("config.yml", 'r') as stream:
        try:
            parsedYaml=yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    server = parsedYaml['server'][server]
    database = parsedYaml['database'][db]
    username = parsedYaml['credentials']['username']
    password = parsedYaml['credentials']['password']

    conn_str='DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
    return conn_str


def save_dataframe_to_sql(dataframe, table_name, connection_string):
    """
    Save a pandas DataFrame to an existing table in an SQL database using 'insert into'.

    Parameters:
        - dataframe: The DataFrame to be saved.
        - table_name: The name of the existing table in the SQL database.
        - connection_string: The connection string for the SQL database.

    Returns:
        None (The DataFrame will be saved in the specified table.)
    """

    try:
        # Establish the database connection using pyodbc.
        conn = pyodbc.connect(connection_string)

        # Create a cursor to execute SQL commands.
        cursor = conn.cursor()

        # Convert the DataFrame to a list of tuples to be used with the 'executemany' method.
        data_tuples = [tuple(row) for row in dataframe.itertuples(index=False, name=None)]

        # Generate placeholders for the SQL query (based on the number of columns in the DataFrame).
        placeholders = ",".join(["?" for _ in dataframe.columns])

        # Construct the SQL insert query.
        insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"

        # Execute the insert query with the data tuples.
        cursor.executemany(insert_query, data_tuples)

        # Commit the changes to the database.
        conn.commit()

        # Close the cursor and the database connection.
        cursor.close()
        conn.close()

        print(f"DataFrame successfully saved to the '{table_name}' table in the SQL database.")

    except Exception as e:
        print("An error occurred:", str(e))

