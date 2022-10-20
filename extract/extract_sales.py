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
channel_conf = confP ['csv']['sales_csv']
con_db_stg = Db_Connection(type, host, port, user, pwd, db)


def ext_sales():
    try:
   
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception (f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception ("Error trying to connect to the b2b_dwh_staging")

        #Dictionary for values 
        sales_dict = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[]
        }
        sales_csv = pd.read_csv(f'csvs/sales.csv')
        #Process CSV Content
        if not sales_csv.empty:
            for id,cuid,tiid,chid,proid,quso,amso \
                in zip(sales_csv['PROD_ID'],sales_csv['CUST_ID'],
                sales_csv['TIME_ID'], sales_csv['CHANNEL_ID'],
                sales_csv['PROMO_ID'],sales_csv['QUANTITY_SOLD'],sales_csv['AMOUNT_SOLD']):
                sales_dict["prod_id"].append(id)
                sales_dict["cust_id"].append(cuid)
                sales_dict["time_id"].append(tiid)
                sales_dict["channel_id"].append(chid)
                sales_dict["promo_id"].append(proid)
                sales_dict["quantity_sold"].append(quso)
                sales_dict["amount_sold"].append(amso)
        if sales_dict["prod_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE sales_ext")
            df_sales_ext = pd.DataFrame(sales_dict)
            df_sales_ext.to_sql('sales_ext', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass