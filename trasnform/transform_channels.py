from util.db_connection import Db_Connection
from util.properties import confStg
import pandas as pd
import traceback

def tran_channels(map_id):
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
        #Dictionary for values of channels_ext
        channel_dict = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[],
            "PROCESOETL_ID":[]

        }
        channel_ext = pd.read_sql("SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_ext", ses_db_stg)
        if not channel_ext.empty:
            for id,des,cla,cla_id \
                in zip(channel_ext['CHANNEL_ID'],channel_ext['CHANNEL_DESC'],
                channel_ext['CHANNEL_CLASS'], channel_ext['CHANNEL_CLASS_ID']
                ):
                channel_dict["channel_id"].append(id)
                channel_dict["channel_desc"].append(des)
                channel_dict["channel_class"].append(cla)
                channel_dict["channel_class_id"].append(cla_id)
                channel_dict["PROCESOETL_ID"].append(map_id)
        if channel_dict["channel_id"]:
                    df_channels_tran = pd.DataFrame(channel_dict)
                    df_channels_tran.to_sql('channels_tran', ses_db_stg, if_exists="append",index=False)
 
    except:
        traceback.print_exc()
    finally:
        pass