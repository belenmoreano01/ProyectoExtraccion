from util.db_connection import Db_Connection
import pandas as pd
import traceback
import configparser

confP = configparser.ConfigParser()
confP.read('data.properties')

type = confP ['parametersMYSQL']['type']
host =confP ['parametersMYSQL']['host']
port = confP ['parametersMYSQL']['port']
user = confP ['parametersMYSQL']['user']
pwd = confP ['parametersMYSQL']['pwd']
db = confP ['parametersMYSQL']['db']
channel_conf = confP ['csv']['countries_csv']
con_db_stg = Db_Connection(type, host, port, user, pwd, db)


def ext_countries():
    try:
   
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception (f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception ("Error trying to connect to the b2b_dwh_staging")

        #Dictionary for values
        countries_dict = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[]
        }
        countries_csv = pd.read_csv(f'csvs/countries.csv')
        #Process CSV Content
        if not countries_csv.empty:
            for id,nam,reg,reg_id \
                in zip(countries_csv['COUNTRY_ID'],countries_csv['COUNTRY_NAME'],
                countries_csv['COUNTRY_REGION'], countries_csv['COUNTRY_REGION_ID']):
                countries_dict["country_id"].append(id)
                countries_dict["country_name"].append(nam)
                countries_dict["country_region"].append(reg)
                countries_dict["country_region_id"].append(reg_id)
        if countries_dict["country_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE countries_ext")
            df_countries_ext = pd.DataFrame(countries_dict)
            df_countries_ext.to_sql('countries_ext', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass