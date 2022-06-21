# Substrate-Analytics

Starting point for Polkadot, Kusama and other Substrate chains to perform analysis on validator and nominator earnings. The script will query on-chain storage data for staking reward history and save the data to a Postgres database.

The end resut will be 2 tables added to your SQL database for each chain. Validator reward history tables will include historical reward points earned, earnings, commission, total bond, nominator reward, nominator APY, validator APY. Nominator history will include all of the same data as the validator table but at the nominator level and will show staking reward history for each on-chain nominator.

1. Clone the repo
2. Create a virtual environment and install requirements `python -m pip install -r requirements.txt`
3. Add environment variables for your Snowflake SQL server connection

`SNOWFLAKE_USER`
`SNOWFLAKE_PASSWORD`
`SNOWFLAKE_ACCOUNT`
`SNOWFLAKE_DB`

Learn more about Python and Snowflake SQLAlchemy connection: https://docs.snowflake.com/en/user-guide/sqlalchemy.html

Optional: Add environment variables for your Postgres SQL server and database

`SQLHOST = your_server_ip`
`DB_USER = username`
`DB_PASSWORD = password`
`DB = database`
`SQLPORT = port`

4. Execute `setup.py` to create a dimension table with the RPC url, ss58 format, and type registry preset. Paramaters for Polkadot and Kusama are included. This file will only need to run once, and can be used anytime you want to add new chains to your update loop.
5. Execute `run.py` to begin harvesting the chain data and loading it to SQL. 

Setup and Run are intended for testing. The update script can be automated by using Google Cloud Functions, PubSub, and Cron Scheduler.
