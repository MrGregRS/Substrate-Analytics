from substrateinterface import SubstrateInterface
import pandas as pd
import sqlalchemy
import os
from datetime import datetime as dt
import pytz

eastern = pytz.timezone('US/Eastern')
date_format = '%m-%d-%y %I:%M %p'

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

class Blockchain():
    def __init__(self, url, ss58_format, type_registry_preset):
        self.url = url
        self.ss58_format = ss58_format
        self.type_registry_preset = type_registry_preset
        self.substrate = SubstrateInterface(url=self.url,ss58_format=self.ss58_format,type_registry_preset=self.type_registry_preset)
        self.substrate.reload_type_registry()
        with self.substrate as substrate:
            self.max_validators = int(str(substrate.query("Staking", "ValidatorCount")))
            self.min_nominator_bond = int(str(substrate.query("Staking", "MinNominatorBond")))
            self.expected_blocktime_seconds = int(str(substrate.get_constant('Babe','ExpectedBlockTime'))) / 1000
            self.blocks_per_epoch = int(str(substrate.get_constant('Babe','EpochDuration')))
            self.sessions_per_era = int(str(substrate.get_constant('Staking','SessionsPerEra')))
            self.nominators_per_validator = int(str(substrate.get_constant('Staking','MaxNominatorRewardedPerValidator')))
            self.active_era = substrate.query('Staking', 'ActiveEra')['index']
            self.history_depth = int(str(substrate.query('Staking', 'HistoryDepth')))
        self.hours_per_era = self.sessions_per_era * self.blocks_per_epoch * self.expected_blocktime_seconds / 60 / 60
        self.eras_per_year = (24 / self.hours_per_era) * 365
        

    def get_stakers(self, era):
        nominators = []
        validator_list = []
        validators = self.substrate.query_map('Staking','ErasStakersClipped',[era])
        for validator, data in validators:
            total = data['total'].value
            own = data['own'].value
            for nominator in data['others']:
                address = nominator['who'].value
                value = nominator['value'].value
                nominators.append([validator.value, address, value])
            validator_list.append([validator.value, total, own, len(data['others'])])
        nominator_df = pd.DataFrame(nominators, columns=['validator', 'nominator', 'bond'])
        validator_df = pd.DataFrame(validator_list, columns=['validator', 'total_bond', 'validator_bond', 'nominators'])
        return nominator_df, validator_df

    def reward_hist(self, start_era=None, end_era=None):
        #If the ending era is not specified, get the current active era on the chain
        if end_era == None:
            end_era = int(str(self.active_era))
        else:
            end_era = int(end_era)
        #If the start era is not specified, determine the starting point
        if start_era == None:
            try:
                #If there is current data stored in SQL, set the starting era to the next era
                table = str(self.type_registry_preset) + '_validator_reward_hist'
                start_era = pool.execute('''SELECT COALESCE(max(era),0) FROM public.{};'''.format(table)).fetchall()[0][0] + 1
                start_era = int(start_era)
            except:
                #If there is no SQL data, set the starting era to the depth of on chain history
                start_era = int(str(self.active_era))-self.history_depth
        else:
            start_era = int(start_era)
        #Create a log to record the results of each era's data load
        update_log = {'chain':self.type_registry_preset}

        #Loop over each era defined in the start to end range
        for era in range(start_era,end_era,1):
            #get the total staking reward paid out to all validators for the era
            era_reward = self.substrate.query('Staking', 'ErasValidatorReward',[era])
            
            #get the reward points earned for each validator in the era
            data = self.substrate.query('Staking', 'ErasRewardPoints',[era])
            total = data['total'].value
            
            #loop over the results and append them to a list
            reward_history = [] #list to save validator reward points
            for validator in data['individual'].value:
                reward_history.append([era,total,validator[0],validator[1]])
            
            #create a data frame from the reward points list
            points_df = pd.DataFrame(reward_history,columns=['era','era_total_reward_points','validator','validator_reward_points'])
            points_df['era_reward'] = int(str(era_reward))

            #calculate the total earnings for each validator - earnings are proportional to the reward points earned
            points_df['total_earnings'] = ((points_df.validator_reward_points / points_df.era_total_reward_points) * points_df.era_reward).astype('int64')

            #get the commission each validator set for the era and add them to a new data frame
            validator_prefs = self.substrate.query_map('Staking','ErasValidatorPrefs',[era])
            validator_list = []
            for validator, commission in validator_prefs:
                address = validator.value
                validator_list.append([address,commission['commission'].value / (10**9),commission['blocked'].value])
            prefs_df = pd.DataFrame(validator_list,columns=['validator','commission','blocked'])

            #get all of the nominator and validators bonds, and the map of nominators to validators
            nominator_df, validator_df = self.get_stakers(era)

            #join the validator commission to the validator data frame
            validator_df = prefs_df.merge(validator_df, left_on='validator', right_on='validator', how='left').merge(points_df, left_on='validator', right_on='validator', how='left').fillna(0)

            #determine the portion of validator earnings that gets paid to nominators by substrating the validator commission
            validator_df['nominator_earnings'] = (validator_df['total_earnings'] * (1-validator_df['commission'])).astype('int64')

            #calculate the APY for nominators based on their bond and earnings
            validator_df['nominator_apy'] = ((1 + (validator_df['nominator_earnings'] / validator_df['total_bond']))** self.eras_per_year)-1
            
            #join the validator data frame to the nominator data frame and calculate the earnings for each nominator
            nominator_df = nominator_df.merge(validator_df, left_on='validator', right_on='validator', how='left')
            nominator_df['staking_rewards'] = (nominator_df['bond'] / nominator_df['total_bond'] * nominator_df['nominator_earnings']).astype('int64')
            validator_df['apy'] = ((1 + (validator_df['total_earnings'] / validator_df['total_bond']))** self.eras_per_year)-1

            #get the current UTC datetime for SQL
            update_utc_datetime = pytz.utc.localize(dt.utcnow())
            validator_df['update_utc_datetime'] = update_utc_datetime
            nominator_df['update_utc_datetime'] = update_utc_datetime

            #upload the data to SQL - create the tables if they do not exist
            try:
                if len(validator_df) > 100:
                    with pool.begin() as connection: #open connection to SQL as a transaction - if failed the query will get rolled back

                        #upload the validator data - if the data for the era already exists, restate the data for that era and delete the old data
                        table = str(self.type_registry_preset) + '_validator_reward_hist'
                        validator_df.to_sql(table, con=connection, if_exists='append',index=False)
                        connection.execute('''DELETE FROM public.{} WHERE "era" = '{}' AND ("update_utc_datetime" is NULL or "update_utc_datetime" < '{}');'''.format(table, era, update_utc_datetime))

                        #upload the nominator data - if the data for the era already exists, restate the data for that era and delete the old data
                        table = str(self.type_registry_preset) + '_nominator_reward_hist'
                        nominator_df.to_sql(table, con=connection, if_exists='append',index=False)
                        connection.execute('''DELETE FROM public.{} WHERE "era" = '{}' AND ("update_utc_datetime" is NULL or "update_utc_datetime" < '{}');'''.format(table, era, update_utc_datetime))
                    update_log[era] = 'Success'
                else:
                    update_log[era] = 'No Data'
            except Exception as e:
                print('Error at SQL upload')
                print(e)
                update_log[era] = 'Error'
        return update_log

def reward_points_sql(con):
    chain = Blockchain(url=con['url'],ss58_format=con['ss58_format'],type_registry_preset=con['type_registry_preset'])
    result = chain.reward_hist()
    return result

if __name__=='__main__':
    ####### POLKADOT #####
    con = {
        'url': "wss://rpc.polkadot.io",
        'ss58_format': 0,
        'type_registry_preset':'polkadot'
    }
    result = reward_points_sql(con)
    print(result)