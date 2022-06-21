from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import os

SNOWFLAKE_USER = os.environ.get('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_DB = os.environ.get('SNOWFLAKE_DB')

engine = create_engine(
    URL(
        user = SNOWFLAKE_USER,
        password = SNOWFLAKE_PASSWORD,
        account = SNOWFLAKE_ACCOUNT,
        database = SNOWFLAKE_DB,
        schema = 'public',
        warehouse = 'COMPUTE_WH',
    )
)

if __name__=='__main__':
    try:
        connection = engine.connect()
        results = connection.execute('select current_version()').fetchone()
        print(results[0])
    finally:
        connection.close()
        engine.dispose()