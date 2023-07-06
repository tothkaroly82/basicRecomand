#%%
import pandas as pd
import pyodbc
from sqlSelectToDataFrame import sqlSelectToDataFrame
from sklearn.preprocessing import StandardScaler

# %% read data from database
df_items_clients_orig=sqlSelectToDataFrame('sql_select.sql',0)

#%% drop unnecessary columns
columns_to_drop=['Valoare','Qty']
df_items_clients=df_items_clients_orig.drop(columns=columns_to_drop)
columns_to_drop_existing = [col for col in columns_to_drop if col in df_items_clients.columns]
df_items_clients.drop(columns_to_drop_existing, axis=1)
df_items_clients=df_items_clients[df_items_clients['NrDoc'] >= 1]

#%% standarize NrDoc value
scaler = StandardScaler()
df_items_clients['NrDocStd'] = df_items_clients.groupby('ClientId')['NrDoc'].transform(lambda x: scaler.fit_transform(x.values.reshape(-1, 1)).flatten())

#%%save result to excel
df_items_clients['Campain']='2023LichidareStoc'
df_items_clients.to_excel('filtered_data.xlsx', index=False)

#-----------------------Statistics and representation--------------------------------------------------------------------

#%%
item_stats = df_items_clients.groupby('ItemId')['NrDoc'].agg(['mean', 'std','count'])
item_stats= item_stats.sort_values('count', ascending=False)


