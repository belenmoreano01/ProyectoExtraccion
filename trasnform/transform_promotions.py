from util.db_connection import Db_Connection
from util.properties import confStg
import pandas as pd
import traceback

from trasnform.transformacion import change_date

def tran_promotions(map_id):
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
        promotions_dict = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[],
            "procesoetl_id":[]
        }
        promotions_ext = pd.read_sql("SELECT PROMO_ID,PROMO_NAME,PROMO_COST,PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions_ext", ses_db_stg)
        if not promotions_ext.empty:
            for id,pna,pco,pbeda,penda \
                in zip(promotions_ext['PROMO_ID'],promotions_ext['PROMO_NAME'],
                promotions_ext['PROMO_COST'], promotions_ext['PROMO_BEGIN_DATE'],
                promotions_ext['PROMO_END_DATE']):
                promotions_dict["promo_id"].append(id)
                promotions_dict["promo_name"].append(pna)
                promotions_dict["promo_cost"].append(pco)
                promotions_dict["promo_begin_date"].append(change_date(pbeda))
                promotions_dict["promo_end_date"].append(change_date(penda))
                promotions_dict["procesoetl_id"].append(map_id)
        if promotions_dict["promo_id"]:
            df_promotions_tran = pd.DataFrame(promotions_dict)
            df_promotions_tran.to_sql('promotions_tran', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass