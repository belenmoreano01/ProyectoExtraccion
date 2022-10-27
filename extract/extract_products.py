from util.db_connection import Db_Connection
from util.properties import confStg,path
import pandas as pd
import traceback

def ext_products():
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
            
        }
        products_csv = pd.read_csv(f'{ruta}products.csv')
        
        if not products_csv.empty:
            for id,pname,pdesc,pcate,pcateid,pcatedesc,pwecl,supid,psta,plistp,pminp \
                in zip( products_csv['PROD_ID'], products_csv['PROD_NAME'],
                 products_csv['PROD_DESC'],  products_csv['PROD_CATEGORY'],
                 products_csv['PROD_CATEGORY_ID'],products_csv['PROD_CATEGORY_DESC'],products_csv['PROD_WEIGHT_CLASS'],
                 products_csv['SUPPLIER_ID'],products_csv['PROD_STATUS'],products_csv['PROD_LIST_PRICE'],
                 products_csv['PROD_MIN_PRICE']):
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
        if products_dict["prod_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE products_ext")
            df_products_ext = pd.DataFrame(products_dict)
            df_products_ext.to_sql('products_ext', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass