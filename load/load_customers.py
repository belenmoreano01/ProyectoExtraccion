from util.db_connection import Db_Connection
from util.properties import confSor,confStg
import pandas as pd
import traceback

from trasnform.transformacion import list_countries

def load_customers(map_id):
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
        customers_dict_tran = {
            "cust_id":[],
            "cust_first_name":[],
            "cust_last_name":[],
            "cust_gender":[],
            "cust_year_of_birth":[],
            "cust_marital_status":[],
            "cust_street_address":[],
            "cust_postal_code":[],
            "cust_city":[],
            "cust_state_province":[],
            "country_id":[],
            "cust_main_phone_integer":[],
            "cust_income_level":[],
            "cust_credit_limit":[],
            "cust_email":[]
        #Dictionary for values
        }
        customers_dict_sor = {
            "cust_id":[],
            "cust_first_name":[],
            "cust_last_name":[],
            "cust_gender":[],
            "cust_year_of_birth":[],
            "cust_marital_status":[],
            "cust_street_address":[],
            "cust_postal_code":[],
            "cust_city":[],
            "cust_state_province":[],
            "country_id":[],
            "cust_main_phone_integer":[],
            "cust_income_level":[],
            "cust_credit_limit":[],
            "cust_email":[]

        }
        customers_tran = pd.read_sql(f"SELECT CUST_ID, CUST_FIRST_NAME,CUST_LAST_NAME,CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE,CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_INTEGER,CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT,CUST_EMAIL  FROM customers_tran where PROCESOETL_ID={map_id}", ses_db_stg)
        customers_sor = pd.read_sql("SELECT CUST_ID, CUST_FIRST_NAME,CUST_LAST_NAME,CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE,CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_INTEGER,CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT,CUST_EMAIL  FROM customers_dim", ses_db_sor)
        contries_sor=pd.read_sql(f"SELECT ID,COUNTRY_ID FROM countries_dim", ses_db_sor)
        
        if not customers_tran.empty:
            for id,first_nam,last_nam,gen,yearb,marist,stadr,posco,cit,stpr,countid,maphin,inle,creli,email \
                in zip( customers_tran['CUST_ID'], customers_tran['CUST_FIRST_NAME'],
                 customers_tran['CUST_LAST_NAME'],  customers_tran['CUST_GENDER'],
                 customers_tran['CUST_YEAR_OF_BIRTH'],customers_tran['CUST_MARITAL_STATUS'],customers_tran['CUST_STREET_ADDRESS'],
                 customers_tran['CUST_POSTAL_CODE'],customers_tran['CUST_CITY'],customers_tran['CUST_STATE_PROVINCE'],customers_tran['COUNTRY_ID'],
                 customers_tran['CUST_MAIN_PHONE_INTEGER'],customers_tran['CUST_INCOME_LEVEL'],
                 customers_tran['CUST_CREDIT_LIMIT'],customers_tran['CUST_EMAIL']):
                customers_dict_tran["cust_id"].append(id)
                customers_dict_tran["cust_first_name"].append(first_nam)
                customers_dict_tran["cust_last_name"].append(last_nam)
                customers_dict_tran["cust_gender"].append(gen)
                customers_dict_tran["cust_year_of_birth"].append(yearb)
                customers_dict_tran["cust_marital_status"].append(marist)
                customers_dict_tran["cust_street_address"].append(stadr)
                customers_dict_tran["cust_postal_code"].append(posco)
                customers_dict_tran["cust_city"].append(cit)
                customers_dict_tran["cust_state_province"].append(stpr)
                customers_dict_tran["country_id"].append(list_countries(countid,contries_sor))
                customers_dict_tran["cust_main_phone_integer"].append(maphin)
                customers_dict_tran["cust_income_level"].append(inle)
                customers_dict_tran["cust_credit_limit"].append(creli)
                customers_dict_tran["cust_email"].append(email)

        if not customers_sor.empty:
            for id,first_nam,last_nam,gen,yearb,marist,stadr,posco,cit,stpr,countid,maphin,inle,creli,email \
                in zip( customers_sor['CUST_ID'], customers_sor['CUST_FIRST_NAME'],
                 customers_sor['CUST_LAST_NAME'],  customers_sor['CUST_GENDER'],
                 customers_sor['CUST_YEAR_OF_BIRTH'],customers_sor['CUST_MARITAL_STATUS'],customers_sor['CUST_STREET_ADDRESS'],
                 customers_sor['CUST_POSTAL_CODE'],customers_sor['CUST_CITY'],customers_sor['CUST_STATE_PROVINCE'],customers_sor['COUNTRY_ID'],
                 customers_sor['CUST_MAIN_PHONE_INTEGER'],customers_sor['CUST_INCOME_LEVEL'],
                 customers_sor['CUST_CREDIT_LIMIT'],customers_sor['CUST_EMAIL']):
                customers_dict_sor["cust_id"].append(id)
                customers_dict_sor["cust_first_name"].append(first_nam)
                customers_dict_sor["cust_last_name"].append(last_nam)
                customers_dict_sor["cust_gender"].append(gen)
                customers_dict_sor["cust_year_of_birth"].append(yearb)
                customers_dict_sor["cust_marital_status"].append(marist)
                customers_dict_sor["cust_street_address"].append(stadr)
                customers_dict_sor["cust_postal_code"].append(posco)
                customers_dict_sor["cust_city"].append(cit)
                customers_dict_sor["cust_state_province"].append(stpr)
                customers_dict_sor["country_id"].append(countid)
                customers_dict_sor["cust_main_phone_integer"].append(maphin)
                customers_dict_sor["cust_income_level"].append(inle)
                customers_dict_sor["cust_credit_limit"].append(creli)
                customers_dict_sor["cust_email"].append(email)
                
        if customers_dict_sor["cust_id"]:
            df_customers_tra = pd.DataFrame(customers_dict_tran)
            df_customers_sor = pd.DataFrame(customers_dict_sor)
            fusion = df_customers_tra.merge(df_customers_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            fusion.to_sql('customers_dim', ses_db_sor, if_exists="append",index=False)
            
        else:
            df_customers_tra = pd.DataFrame(customers_dict_tran)
            df_customers_tra.to_sql('customers_dim', ses_db_sor, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass