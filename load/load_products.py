from util.db_connection import Db_Connection
from util.properties import confStg,confSor
import pandas as pd
import traceback

def load_products(map_id):
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
        products_dict_tran = {
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

         #Dictionary for values   
        }
        products_dict_sor = {
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
        products_tran = pd.read_sql(f"SELECT PROD_ID,PROD_NAME,PROD_DESC,PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC,PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS,PROD_LIST_PRICE,PROD_MIN_PRICE FROM products_tran where PROCESOETL_ID={map_id}", ses_db_stg)
        products_sor = pd.read_sql(f"SELECT PROD_ID,PROD_NAME,PROD_DESC,PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC,PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS,PROD_LIST_PRICE,PROD_MIN_PRICE FROM products_dim ", ses_db_sor)
        if not products_tran.empty:
            for id,pname,pdesc,pcate,pcateid,pcatedesc,pwecl,supid,psta,plistp,pminp \
                in zip( products_tran['PROD_ID'], products_tran['PROD_NAME'],
                 products_tran['PROD_DESC'],  products_tran['PROD_CATEGORY'],
                 products_tran['PROD_CATEGORY_ID'],products_tran['PROD_CATEGORY_DESC'],products_tran['PROD_WEIGHT_CLASS'],
                 products_tran['SUPPLIER_ID'],products_tran['PROD_STATUS'],products_tran['PROD_LIST_PRICE'],
                 products_tran['PROD_MIN_PRICE']):
                products_dict_tran["prod_id"].append(id)
                products_dict_tran["prod_name"].append(pname)
                products_dict_tran["prod_desc"].append(pdesc)
                products_dict_tran["prod_category"].append(pcate)
                products_dict_tran["prod_category_id"].append(pcateid)
                products_dict_tran["prod_category_desc"].append(pcatedesc)
                products_dict_tran["prod_weight_class"].append(pwecl)
                products_dict_tran["supplier_id"].append(supid)
                products_dict_tran["prod_status"].append(psta)
                products_dict_tran["prod_list_price"].append(plistp)
                products_dict_tran["prod_min_price"].append(pminp)
        if not products_sor.empty:
            for id,pname,pdesc,pcate,pcateid,pcatedesc,pwecl,supid,psta,plistp,pminp \
                in zip( products_sor['PROD_ID'], products_sor['PROD_NAME'],
                 products_sor['PROD_DESC'],  products_sor['PROD_CATEGORY'],
                 products_sor['PROD_CATEGORY_ID'],products_sor['PROD_CATEGORY_DESC'],products_sor['PROD_WEIGHT_CLASS'],
                 products_sor['SUPPLIER_ID'],products_sor['PROD_STATUS'],products_sor['PROD_LIST_PRICE'],
                 products_sor['PROD_MIN_PRICE']):
                products_dict_sor["prod_id"].append(id)
                products_dict_sor["prod_name"].append(pname)
                products_dict_sor["prod_desc"].append(pdesc)
                products_dict_sor["prod_category"].append(pcate)
                products_dict_sor["prod_category_id"].append(pcateid)
                products_dict_sor["prod_category_desc"].append(pcatedesc)
                products_dict_sor["prod_weight_class"].append(pwecl)
                products_dict_sor["supplier_id"].append(supid)
                products_dict_sor["prod_status"].append(psta)
                products_dict_sor["prod_list_price"].append(plistp)
                products_dict_sor["prod_min_price"].append(pminp)
                
        if products_dict_sor["prod_id"]:
            df_products_tra = pd.DataFrame(products_dict_tran)
            df_products_sor = pd.DataFrame(products_dict_sor)
            fusion = df_products_tra.merge(df_products_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            fusion.to_sql('products_dim', ses_db_sor, if_exists="append",index=False)
            
        else:
            df_products_tra = pd.DataFrame(products_dict_tran)
            df_products_tra.to_sql('products_dim', ses_db_sor, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass