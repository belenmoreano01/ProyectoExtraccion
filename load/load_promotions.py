from util.db_connection import Db_Connection
from util.properties import confStg,confSor
import pandas as pd
import traceback

from trasnform.transformacion import change_date

def load_promotions(map_id):
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
        
        conf_sor= confSor()
        type = conf_sor['TYPE']
        host = conf_sor['HOST']
        port = conf_sor['PORT']
        user = conf_sor['USER']
        pwd = conf_sor['PASSWORD']
        db = conf_sor['DATABASE']
        
        con_db_sor = Db_Connection(type,host,port,user,pwd,db)
        ses_db_sor = con_db_sor.start()
        if ses_db_sor == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_sor == -2:
            raise Exception("Error trying connect to the database")
        #Dictionary for values 
        promotions_dict_tran = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[]
        }
        #Dictionary for values 
        promotions_dict_sor = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[]
        }
        promotions_tran = pd.read_sql(f"SELECT PROMO_ID,PROMO_NAME,PROMO_COST,PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions_tran where PROCESOETL_ID={map_id}", ses_db_stg)
        promotions_sor = pd.read_sql(f"SELECT PROMO_ID,PROMO_NAME,PROMO_COST,PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions_dim", ses_db_sor)
        
        if not promotions_tran.empty:
            for id,pna,pco,pbeda,penda \
                in zip(promotions_tran['PROMO_ID'],promotions_tran['PROMO_NAME'],
                promotions_tran['PROMO_COST'], promotions_tran['PROMO_BEGIN_DATE'],
                promotions_tran['PROMO_END_DATE']):
                promotions_dict_tran["promo_id"].append(id)
                promotions_dict_tran["promo_name"].append(pna)
                promotions_dict_tran["promo_cost"].append(pco)
                promotions_dict_tran["promo_begin_date"].append(pbeda)
                promotions_dict_tran["promo_end_date"].append(penda)
        if not promotions_sor.empty:
            for id,pna,pco,pbeda,penda \
                in zip(promotions_sor['PROMO_ID'],promotions_sor['PROMO_NAME'],
                promotions_sor['PROMO_COST'], promotions_sor['PROMO_BEGIN_DATE'],
                promotions_sor['PROMO_END_DATE']):
                promotions_dict_sor["promo_id"].append(id)
                promotions_dict_sor["promo_name"].append(pna)
                promotions_dict_sor["promo_cost"].append(pco)
                promotions_dict_sor["promo_begin_date"].append(pbeda)
                promotions_dict_sor["promo_end_date"].append(penda)
        if promotions_dict_sor["promo_id"]:
            df_promotions_tra = pd.DataFrame(promotions_dict_tran)
            df_promotions_sor = pd.DataFrame(promotions_dict_sor)
            fusion = df_promotions_tra.merge(df_promotions_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            fusion.to_sql('promotions_dim', ses_db_sor, if_exists="append",index=False)
            
        else:
            df_promotions_tra = pd.DataFrame(promotions_dict_tran)
            df_promotions_tra.to_sql('promotions_dim', ses_db_sor, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass