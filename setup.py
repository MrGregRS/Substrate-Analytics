import pandas as pd

from sqlengine.snowflake import engine as pool

#setup the connection parameters for the substrate chain RPC 
polkadot = ['wss://rpc.polkadot.io', 0, 'polkadot']
kusama = ['wss://kusama-rpc.polkadot.io', 2, 'kusama']

#add to a data frame 
chains_list = [polkadot, kusama]
chains = pd.DataFrame(chains_list, columns=['url', 'ss58_format', 'type_registry_preset'])

#upload the chains to SQL
with pool.begin() as connection:
    chains.to_sql('dim_chains', con=connection, if_exists='replace',index=False)
