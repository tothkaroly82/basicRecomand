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

    server = parsedYaml['server']
    database = parsedYaml['database']
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
