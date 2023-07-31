# %%
import pandas as pd
import sql_statmanets as sql
import yaml
import pyodbc

#%%

#%%
connection_string=sql.create_connection_string('mdbwh','ProductCampaigns')
sql.delete_data_from_table(connection_string,'client_product_campaign')





#%%
import sql_statmanets as sql
connection_string=sql.create_connection_string('mdbwh','ProductCampaigns')

# %%
