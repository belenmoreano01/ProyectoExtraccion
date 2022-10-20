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
channel_conf = confP ['csv']['customers_csv']
con_db_stg = Db_Connection(type, host, port, user, pwd, db)


def ext_customers():
    try:
   
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception (f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception ("Error trying to connect to the b2b_dwh_staging")

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
            "cust_email":[]

        }
        customers_csv = pd.read_csv(f'csvs/customers.csv')
        #Process CSV Content
        if not customers_csv.empty:
            for id,first_nam,last_nam,gen,yearb,marist,stadr,posco,cit,stpr,countid,maphin,inle,creli,email \
                in zip( customers_csv['CUST_ID'], customers_csv['CUST_FIRST_NAME'],
                 customers_csv['CUST_LAST_NAME'],  customers_csv['CUST_GENDER'],
                 customers_csv['CUST_YEAR_OF_BIRTH'],customers_csv['CUST_MARITAL_STATUS'],customers_csv['CUST_STREET_ADDRESS'],
                 customers_csv['CUST_POSTAL_CODE'],customers_csv['CUST_CITY'],customers_csv['CUST_STATE_PROVINCE'],customers_csv['COUNTRY_ID'],
                 customers_csv['CUST_MAIN_PHONE_NUMBER'],customers_csv['CUST_INCOME_LEVEL'],
                 customers_csv['CUST_CREDIT_LIMIT'],customers_csv['CUST_EMAIL']):
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
        if customer_dict["cust_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE customers_ext")
            df_customer_ext = pd.DataFrame(customer_dict)
            df_customer_ext.to_sql('customers_ext', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass