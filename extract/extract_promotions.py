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
channel_conf = confP ['csv']['promotions_csv']
con_db_stg = Db_Connection(type, host, port, user, pwd, db)


def ext_promotions():
    try:
   
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception (f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception ("Error trying to connect to the b2b_dwh_staging")

        #Dictionary for values 
        promotions_dict = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[]
        }
        promotions_csv = pd.read_csv(f'csvs/promotions.csv')
        #Process CSV Content
        if not promotions_csv.empty:
            for id,pna,pco,pbeda,penda \
                in zip(promotions_csv['PROMO_ID'],promotions_csv['PROMO_NAME'],
                promotions_csv['PROMO_COST'], promotions_csv['PROMO_BEGIN_DATE'],
                promotions_csv['PROMO_END_DATE']):
                promotions_dict["promo_id"].append(id)
                promotions_dict["promo_name"].append(pna)
                promotions_dict["promo_cost"].append(pco)
                promotions_dict["promo_begin_date"].append(pbeda)
                promotions_dict["promo_end_date"].append(penda)
        if promotions_dict["promo_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE promotions_ext")
            df_promotions_ext = pd.DataFrame(promotions_dict)
            df_promotions_ext.to_sql('promotions_ext', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass      