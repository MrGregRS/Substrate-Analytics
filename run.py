import pandas as pd
import concurrent.futures
from functions.stakers import reward_points_sql as rp
from sqlengine.snowflake import engine as pool


with pool.begin() as connection:
    df = pd.read_sql_table('dim_chains',connection)

with concurrent.futures.ThreadPoolExecutor() as executor:
    results = [executor.submit(rp, record) for record in df.to_dict('records')]
    for result in concurrent.futures.as_completed(results):
        print(result.result())