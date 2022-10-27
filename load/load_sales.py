from util.db_connection import Db_Connection
from util.properties import confStg,confSor,path
import pandas as pd
import traceback


def load_sales(map_id):
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
        sales_dict_tran = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[],
        #Dictionary for values    
        }
        sales_dict_sor = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[],
           
        }
        sales_tran = pd.read_sql(f"SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD  FROM sales_tran where PROCESOETL_ID={map_id}", ses_db_stg)
        sales_sor = pd.read_sql(f"SELECT PROD_ID,CUST_ID,TIME_ID,CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD  FROM sales", ses_db_sor)
        product_sor=pd.read_sql(f"SELECT ID,PROD_ID FROM products_dim", ses_db_sor)
        customer_sor=pd.read_sql(f"SELECT ID,CUST_ID FROM customers_dim", ses_db_sor)
        time_sor=pd.read_sql(f"SELECT ID,TIME_ID FROM times_dim", ses_db_sor)
        channel_sor=pd.read_sql(f"SELECT ID,CHANNEL_ID FROM channels_dim", ses_db_sor)
        promo_sor=pd.read_sql(f"SELECT ID,PROMO_ID FROM promotions_dim", ses_db_sor)

        dict_product=dict()
        if not product_sor.empty:
            for id, proid\
                in zip(product_sor['ID'],product_sor['PROD_ID']):
                dict_product[proid] = id

        dict_customer=dict()
        if not customer_sor.empty:
            for id, cusid\
                in zip(customer_sor['ID'],customer_sor['CUST_ID']):
                dict_customer[cusid] = id

        dict_time=dict()
        if not time_sor.empty:
            for id, timid\
                in zip(time_sor['ID'],time_sor['TIME_ID']):
                dict_time[timid] = id

        dict_channel=dict()
        if not channel_sor.empty:
            for id, chid\
                in zip(channel_sor['ID'],channel_sor['CHANNEL_ID']):
                dict_channel[chid] = id

        dict_promotions=dict()
        if not promo_sor.empty:
            for id, proid\
                in zip(promo_sor['ID'],promo_sor['PROMO_ID']):
                dict_promotions[proid] = id

        if not sales_tran.empty:
            for id,cuid,tiid,chid,proid,quso,amso \
                in zip(sales_tran['PROD_ID'],sales_tran['CUST_ID'],
                sales_tran['TIME_ID'], sales_tran['CHANNEL_ID'],
                sales_tran['PROMO_ID'],sales_tran['QUANTITY_SOLD'],sales_tran['AMOUNT_SOLD']):
                sales_dict_tran["prod_id"].append(dict_product[id])
                sales_dict_tran["cust_id"].append(dict_customer[cuid])
                sales_dict_tran["time_id"].append(dict_time[tiid])
                sales_dict_tran["channel_id"].append(dict_channel[chid])
                sales_dict_tran["promo_id"].append(dict_promotions[proid])
                sales_dict_tran["quantity_sold"].append(quso)
                sales_dict_tran["amount_sold"].append(amso)
        if not sales_sor.empty:
            for id,cuid,tiid,chid,proid,quso,amso \
                in zip(sales_sor['PROD_ID'],sales_sor['CUST_ID'],
                sales_sor['TIME_ID'], sales_sor['CHANNEL_ID'],
                sales_sor['PROMO_ID'],sales_sor['QUANTITY_SOLD'],sales_sor['AMOUNT_SOLD']):
                sales_dict_sor["prod_id"].append(id)
                sales_dict_sor["cust_id"].append(cuid)
                sales_dict_sor["time_id"].append(tiid)
                sales_dict_sor["channel_id"].append(chid)
                sales_dict_sor["promo_id"].append(proid)
                sales_dict_sor["quantity_sold"].append(quso)
                sales_dict_sor["amount_sold"].append(amso)
        if sales_dict_sor["prod_id"]:
            df_sales_tra = pd.DataFrame(sales_dict_tran)
            df_sales_sor = pd.DataFrame(sales_dict_sor)
            fusion = df_sales_tra.merge(df_sales_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            fusion.to_sql('sales', ses_db_sor, if_exists="append",index=False)
            
        else:
            df_sales_tra = pd.DataFrame(sales_dict_tran)
            df_sales_tra.to_sql('sales', ses_db_sor, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass