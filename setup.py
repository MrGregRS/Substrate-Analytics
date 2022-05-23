import pandas as pd
import sqlalchemy
import os

HOST = os.environ.get('SQLHOST')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB = os.environ.get('DB')
PORT = os.environ.get('SQLPORT')

pool = sqlalchemy.create_engine(
        sqlalchemy.engine.url.URL.create(
        drivername="postgresql+psycopg2",
        username=DB_USER,
        password=DB_PASSWORD,
        host=HOST,
        port=PORT,
        database=DB))


#setup the connection parameters for the substrate chain RPC 
polkadot = ['wss://rpc.polkadot.io', 0, 'polkadot']
kusama = ['wss://kusama-rpc.polkadot.io', 2, 'kusama']

#add to a data frame 
chains_list = [polkadot, kusama]
chains = pd.DataFrame(chains_list, columns=['url', 'ss58_format', 'type_registry_preset'])

#upload the chains to SQL
with pool.begin() as connection:
    chains.to_sql('dim_chains', con=connection, if_exists='replace',index=False)
