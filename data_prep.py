#%%
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sqlSelectToDataFrame import sqlSelectToDataFrame

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

    def standardize_nr_doc_value(self):
        scaler = StandardScaler()
        self.df_items_clients['NrDocStd'] = self.df_items_clients.groupby('ClientId')['NrDoc'].transform(
            lambda x: scaler.fit_transform(x.values.reshape(-1, 1)).flatten()
        )

    def generate_standardized_rank(self, sql_filename, nr_of_sql, columns_to_drop):
        self.get_data_from_sql(sql_filename, nr_of_sql)
        self.drop_unnecessary_columns(columns_to_drop)
        self.filter_empty_line()
        self.standardize_nr_doc_value()
        return self.df_items_clients
#%%
item_rank_processor = ClientItemsRank()
item_rank = item_rank_processor.generate_standardized_rank('sql_select.sql', 0, ['Valoare', 'Qty'])
#%%
sintetic_rank_processor = ClientItemsRank()
sintetic_rank = sintetic_rank_processor.generate_standardized_rank('sql_select.sql', 0, ['Valoare', 'Qty'])


# %%
