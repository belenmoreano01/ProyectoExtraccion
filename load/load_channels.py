from util.db_connection import Db_Connection
from util.properties import confSor,confStg
import pandas as pd
import traceback

def load_channels(map_id):
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
        channels_dict_tran = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[]
        }
        #Dictionary for values 
        channels_dict_sor = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[]
        }
        channel_tran = pd.read_sql(f"SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_tran where PROCESOETL_ID={map_id} ", ses_db_stg)
        channel_sor = pd.read_sql(f"SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_dim  ", ses_db_sor)
        if not channel_tran.empty:
            for id,des,cla,cla_id \
                in zip(channel_tran['CHANNEL_ID'],channel_tran['CHANNEL_DESC'],
                channel_tran['CHANNEL_CLASS'], channel_tran['CHANNEL_CLASS_ID']
                ):
                channels_dict_tran["channel_id"].append(id)
                channels_dict_tran["channel_desc"].append(des)
                channels_dict_tran["channel_class"].append(cla)
                channels_dict_tran["channel_class_id"].append(cla_id)

        if not channel_sor.empty:
            for id,des,cla,cla_id \
                in zip(channel_sor['CHANNEL_ID'],channel_sor['CHANNEL_DESC'],
                channel_sor['CHANNEL_CLASS'], channel_sor['CHANNEL_CLASS_ID']
                ):
                channels_dict_sor["channel_id"].append(id)
                channels_dict_sor["channel_desc"].append(des)
                channels_dict_sor["channel_class"].append(cla)
                channels_dict_sor["channel_class_id"].append(cla_id)
        if channels_dict_sor["channel_id"]:
            df_channels_tra = pd.DataFrame(channels_dict_tran)
            df_channels_sor = pd.DataFrame(channels_dict_sor)
            fusion = df_channels_tra.merge(df_channels_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            fusion.to_sql('channels_dim', ses_db_sor, if_exists="append",index=False)
            
        else:
            df_channels_tra = pd.DataFrame(channels_dict_tran)
            df_channels_tra.to_sql('channels_dim', ses_db_sor, if_exists="append",index=False)
            



    except:
        traceback.print_exc()
    finally:
        pass