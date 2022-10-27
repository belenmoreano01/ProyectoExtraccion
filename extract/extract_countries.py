from util.db_connection import Db_Connection
from util.properties import confStg,path
import pandas as pd
import traceback

def ext_countries():
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
        countries_dict = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[]
        }
        countries_csv = pd.read_csv(f'{ruta}countries.csv')
    
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