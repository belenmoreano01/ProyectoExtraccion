from util.db_connection import Db_Connection
from util.properties import confStg
import pandas as pd
import traceback

def tran_countries(map_id):
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
        countries_dict = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[],
            "procesoetl_id":[]
            
        }
        countries_ext = pd.read_sql("SELECT COUNTRY_ID, COUNTRY_NAME, COUNTRY_REGION, COUNTRY_REGION_ID FROM countries_ext", ses_db_stg)
        
        if not countries_ext.empty:
            for id,nam,reg,reg_id \
                in zip(countries_ext['COUNTRY_ID'],countries_ext['COUNTRY_NAME'],
                countries_ext['COUNTRY_REGION'], countries_ext['COUNTRY_REGION_ID']):
                countries_dict["country_id"].append(id)
                countries_dict["country_name"].append(nam)
                countries_dict["country_region"].append(reg)
                countries_dict["country_region_id"].append(reg_id)
                countries_dict["procesoetl_id"].append(map_id)
        if countries_dict["country_id"]:
            df_countries_tran = pd.DataFrame(countries_dict)
            df_countries_tran.to_sql('countries_tran', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass
    