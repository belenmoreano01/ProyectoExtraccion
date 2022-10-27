from util.db_connection import Db_Connection
from util.properties import confStg
import pandas as pd
import traceback

def tran_products(map_id):
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
        products_dict = {
            "prod_id":[],
            "prod_name":[],
            "prod_desc":[],
            "prod_category":[],
            "prod_category_id":[],
            "prod_category_desc":[],
            "prod_weight_class":[],
            "supplier_id":[],
            "prod_status":[],
            "prod_list_price":[],
            "prod_min_price":[],
            "procesoetl_id":[]
            
        }
        products_ext = pd.read_sql("SELECT PROD_ID,PROD_NAME,PROD_DESC,PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC,PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS,PROD_LIST_PRICE,PROD_MIN_PRICE FROM products_ext", ses_db_stg)
        if not products_ext.empty:
            for id,pname,pdesc,pcate,pcateid,pcatedesc,pwecl,supid,psta,plistp,pminp \
                in zip( products_ext['PROD_ID'], products_ext['PROD_NAME'],
                 products_ext['PROD_DESC'],  products_ext['PROD_CATEGORY'],
                 products_ext['PROD_CATEGORY_ID'],products_ext['PROD_CATEGORY_DESC'],products_ext['PROD_WEIGHT_CLASS'],
                 products_ext['SUPPLIER_ID'],products_ext['PROD_STATUS'],products_ext['PROD_LIST_PRICE'],
                 products_ext['PROD_MIN_PRICE']):
                products_dict["prod_id"].append(id)
                products_dict["prod_name"].append(pname)
                products_dict["prod_desc"].append(pdesc)
                products_dict["prod_category"].append(pcate)
                products_dict["prod_category_id"].append(pcateid)
                products_dict["prod_category_desc"].append(pcatedesc)
                products_dict["prod_weight_class"].append(pwecl)
                products_dict["supplier_id"].append(supid)
                products_dict["prod_status"].append(psta)
                products_dict["prod_list_price"].append(plistp)
                products_dict["prod_min_price"].append(pminp)
                products_dict["procesoetl_id"].append(map_id)
        if products_dict["prod_id"]:
            df_products_tran = pd.DataFrame(products_dict)
            df_products_tran.to_sql('products_tran', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass