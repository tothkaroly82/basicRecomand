#%%
import pandas as pd
from sklearn.preprocessing import StandardScaler, MinMaxScaler, MaxAbsScaler
from sql_statmanets import sqlSelectToDataFrame
import sql_statmanets as ss

#%%
class ClientItemsRank:
    def __init__(self):
        self.df_items_clients = None

    def get_data_from_sql(self, sql_filename, nr_of_sql):
        self.df_items_clients = sqlSelectToDataFrame(sql_filename, nr_of_sql)

    def drop_unnecessary_columns(self, columns_to_drop):
        columns_to_drop_existing = [col for col in columns_to_drop if col in self.df_items_clients.columns]
        if len(columns_to_drop_existing) > 0:
            self.df_items_clients.drop(columns_to_drop_existing, axis=1, inplace=True)

    def filter_empty_line(self):
        self.df_items_clients = self.df_items_clients[self.df_items_clients['NrDoc'] >= 1]

    def scale_nr_doc(self):
        scaler = MaxAbsScaler()
        self.df_items_clients['NrDocStd'] = self.df_items_clients.groupby('ClientId')['NrDoc'].transform(
            lambda x: scaler.fit_transform(x.values.reshape(-1, 1)).flatten()
        )


    def generate_standardized_rank(self, sql_filename, nr_of_sql, columns_to_drop):
        self.get_data_from_sql(sql_filename, nr_of_sql)
        self.drop_unnecessary_columns(columns_to_drop)
        self.filter_empty_line()
        self.scale_nr_doc()
        return self.df_items_clients
#%%
item_rank_processor = ClientItemsRank()
rank_byitems = item_rank_processor.generate_standardized_rank('sql_select.sql', 0, ['Qty'])

#%%
sintetic_rank_processor = ClientItemsRank()
rank_bysintetic = sintetic_rank_processor.generate_standardized_rank('sql_select.sql', 1, ['Qty'])


# %% merge df
clients_ranks = rank_bysintetic.merge(rank_byitems, on=['ItemId', 'ClientId'], how='outer', suffixes=('_sintetic', '_item'))

#%% create weighted global rank based on sintetic rank and item rank
clients_ranks['Score global']=0.9*clients_ranks['NrDocStd_item'].fillna(0)+0.1*clients_ranks['NrDocStd_sintetic'].fillna(0)
clients_ranks['Rank'] = clients_ranks.groupby('ClientId')['Score global'].rank(method='first', ascending=False)
clients_ranks = clients_ranks.sort_values(by=['Score global', 'Valoare_item', 'Valoare_sintetic','ItemId'], ascending=[False, False,False, True])
clients_ranks = clients_ranks.reset_index(drop=True)

#----- save results to database--------------------------------------------------------------------------
#%% filter products with rank less than 49
rank_lower48=clients_ranks[clients_ranks['Rank'] <=48]

#%% # rename columns to map name`s of database table and drop unnecesary columns
rank_lower48['CampaignId']='2023LichidareStoc'
rank_lower48['Price']=None
rank_lower48['unit']=None
rank_lower48['sent']=0


column_mapping = {
    'ItemId': 'product_dax_id',
    'ClientId': 'client_dax_id',
    'Rank': 'score',
    'CampaignId':'campaign_dax_id',
    'Unit':'unit',
    'Price':'price',
    'Sent':'sent'
}

rank_lower48.rename(columns=column_mapping, inplace=True)
# keep only necesary columns
columns_to_keep = list(column_mapping.values())
rank_lower48 = rank_lower48[columns_to_keep]
desired_columns_order = ['client_dax_id', 'product_dax_id', 'campaign_dax_id', 'price', 'score', 'sent', 'unit']
rank_lower48 = rank_lower48.reindex(columns=desired_columns_order)


#%% delete existing data
connection_string=ss.create_connection_string('mdbwh','ProductCampaigns')
ss.delete_data_from_table(connection_string,'client_product_campaign')
#%%

#%% insert data
ss.save_dataframe_to_sql(rank_lower48,'client_product_campaign',connection_string)


#------------------save results end-------------



#%%
#%% generate statistics
item_rank_statistics = rank_lower48.groupby('ItemId')
item_rank_statistic = item_rank_statistics['Rank'].agg(['mean', 'min', 'max', 'std','count'])



# %%
