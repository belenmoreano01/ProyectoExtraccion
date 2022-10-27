from util.db_connection import Db_Connection
from util.properties import confStg
import pandas as pd
import traceback

def tran_customers(map_id):
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
        customer_dict = {
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
            "cust_email":[],
            "procesoetl_id":[]

        }
        customers_ext = pd.read_sql("SELECT CUST_ID, CUST_FIRST_NAME,CUST_LAST_NAME,CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE,CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_INTEGER,CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT,CUST_EMAIL  FROM customers_ext", ses_db_stg)

        if not customers_ext.empty:
            for id,first_nam,last_nam,gen,yearb,marist,stadr,posco,cit,stpr,countid,maphin,inle,creli,email \
                in zip( customers_ext['CUST_ID'], customers_ext['CUST_FIRST_NAME'],
                 customers_ext['CUST_LAST_NAME'],  customers_ext['CUST_GENDER'],
                 customers_ext['CUST_YEAR_OF_BIRTH'],customers_ext['CUST_MARITAL_STATUS'],customers_ext['CUST_STREET_ADDRESS'],
                 customers_ext['CUST_POSTAL_CODE'],customers_ext['CUST_CITY'],customers_ext['CUST_STATE_PROVINCE'],customers_ext['COUNTRY_ID'],
                 customers_ext['CUST_MAIN_PHONE_INTEGER'],customers_ext['CUST_INCOME_LEVEL'],
                 customers_ext['CUST_CREDIT_LIMIT'],customers_ext['CUST_EMAIL']):
                customer_dict["cust_id"].append(id)
                customer_dict["cust_first_name"].append(first_nam)
                customer_dict["cust_last_name"].append(last_nam)
                customer_dict["cust_gender"].append(gen)
                customer_dict["cust_year_of_birth"].append(yearb)
                customer_dict["cust_marital_status"].append(marist)
                customer_dict["cust_street_address"].append(stadr)
                customer_dict["cust_postal_code"].append(posco)
                customer_dict["cust_city"].append(cit)
                customer_dict["cust_state_province"].append(stpr)
                customer_dict["country_id"].append(countid)
                customer_dict["cust_main_phone_integer"].append(maphin)
                customer_dict["cust_income_level"].append(inle)
                customer_dict["cust_credit_limit"].append(creli)
                customer_dict["cust_email"].append(email)
                customer_dict["procesoetl_id"].append(map_id)
        if customer_dict["cust_id"]:
            df_customer_ext = pd.DataFrame(customer_dict)
            df_customer_ext.to_sql('customers_tran', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass