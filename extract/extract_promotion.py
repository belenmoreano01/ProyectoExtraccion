from util.db_connection import Db_Connection
from util.properties import confStg,path
import pandas as pd
import traceback

def ext_promotions():
    try:
        
        conf_stg=confStg()
        ruta=path()
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
            "promo_end_date":[]
        }
        promotions_csv = pd.read_csv(f'{ruta}promotions.csv')
        
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