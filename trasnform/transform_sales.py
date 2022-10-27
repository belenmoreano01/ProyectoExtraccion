from util.db_connection import Db_Connection
from util.properties import confStg
import pandas as pd
import traceback

from trasnform.transformacion import change_date

def tran_sales(map_id):
    try:
        conf_stg=confStg()
        type = conf_stg['TYPE']
        host = conf_stg['HOST']
        port = conf_stg['PORT']
        user = conf_stg['USER']
        pwd = conf_stg['PASSWORD']
        db = conf_stg['DATABASE']
        
        con_db_stg = Db_Connection(type,host,port,user,pwd,db)
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying connect to the database")
        #Dictionary for values 
        sales_dict = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[],
            "procesoetl_id":[]
        }
        sales_ext = pd.read_sql("SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD  FROM sales_ext", ses_db_stg)
        if not sales_ext.empty:
            for id,cuid,tiid,chid,proid,quso,amso \
                in zip(sales_ext['PROD_ID'],sales_ext['CUST_ID'],
                sales_ext['TIME_ID'], sales_ext['CHANNEL_ID'],
                sales_ext['PROMO_ID'],sales_ext['QUANTITY_SOLD'],sales_ext['AMOUNT_SOLD']):
                sales_dict["prod_id"].append(id)
                sales_dict["cust_id"].append(cuid)
                sales_dict["time_id"].append(change_date(tiid))
                sales_dict["channel_id"].append(chid)
                sales_dict["promo_id"].append(proid)
                sales_dict["quantity_sold"].append(quso)
                sales_dict["amount_sold"].append(amso)
                sales_dict["procesoetl_id"].append(map_id)
        if sales_dict["prod_id"]:
            df_sales_tran = pd.DataFrame(sales_dict)
            df_sales_tran.to_sql('sales_tran', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass