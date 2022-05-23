from substrateinterface import SubstrateInterface
import sqlalchemy
import pandas as pd
import concurrent.futures
import os
from functions.stakers import reward_points_sql as rp

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

with pool.begin() as connection:
    df = pd.read_sql_table('dim_chains',connection)

with concurrent.futures.ThreadPoolExecutor() as executor:
    results = [executor.submit(rp, record) for record in df.to_dict('records')]
    for result in concurrent.futures.as_completed(results):
        print(result.result())