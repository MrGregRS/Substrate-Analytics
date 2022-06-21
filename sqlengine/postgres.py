import sqlalchemy
import os

HOST = os.environ.get('SQLHOST')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB = os.environ.get('DB')
PORT = os.environ.get('SQLPORT')

engine = sqlalchemy.create_engine(
        sqlalchemy.engine.url.URL.create(
        drivername="postgresql+psycopg2",
        username=DB_USER,
        password=DB_PASSWORD,
        host=HOST,
        port=PORT,
        database=DB))