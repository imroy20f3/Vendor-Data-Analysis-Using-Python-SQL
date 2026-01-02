import pandas as pd 
import os 
from sqlalchemy import create_engine
import time
import logging

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a")


engine = create_engine('sqlite:///inventory.db')
'''This function will ingest the dataframe into database table'''
def ingest_db_csv(data, table_name, engine):
    for chunk in pd.read_csv(data, chunksize=50_000):
        chunk.to_sql(
            table_name,
            con=engine,
            if_exists='append',
            index=False
        )

def load_raw_data():
    '''This function will load the csvs as dataframe and ingest into db'''
    start = time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            df = pd.read_csv('data/'+file)
            logging.info(f'Ingesting {file} in db')
            ingest_db(df, file[:-4], engine)
            end = time.time()
            total_time=(end-start)/60
            logging.info('------Ingestion Complete------')

            logging.info(f'\nTotal Time Taken: {total_time} minutes')

if __name__ == '__main__':

   load_raw_data()