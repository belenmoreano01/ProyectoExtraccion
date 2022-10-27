from util.db_connection import Db_Connection
from util.properties import confStg,confSor
import pandas as pd
import traceback

from trasnform.transformacion import change_date

def load_times(map_id):
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
        times_dict_tran = {
            "time_id":[],
            "day_name":[],
            "day_integer_in_week":[],
            "day_integer_in_month":[],
            "calendar_week_integer":[],
            "calendar_month_integer":[],
            "calendar_month_desc":[],
            "end_of_cal_month":[],
            "calendar_month_name":[],
            "calendar_quarter_desc":[],
            "calendar_year":[]
        }
        #Dictionary for values
        times_dict_sor = {
            "time_id":[],
            "day_name":[],
            "day_integer_in_week":[],
            "day_integer_in_month":[],
            "calendar_week_integer":[],
            "calendar_month_integer":[],
            "calendar_month_desc":[],
            "end_of_cal_month":[],
            "calendar_month_name":[],
            "calendar_quarter_desc":[],
            "calendar_year":[]
        }
        times_tran = pd.read_sql(f"SELECT TIME_ID, DAY_NAME, DAY_INTEGER_IN_WEEK, DAY_INTEGER_IN_MONTH,CALENDAR_WEEK_INTEGER,CALENDAR_MONTH_INTEGER, CALENDAR_MONTH_DESC, END_OF_CAL_MONTH, CALENDAR_QUARTER_DESC, CALENDAR_YEAR FROM times_tran where PROCESOETL_ID={map_id}", ses_db_stg)
        times_sor = pd.read_sql(f"SELECT TIME_ID, DAY_NAME, DAY_INTEGER_IN_WEEK, DAY_INTEGER_IN_MONTH,CALENDAR_WEEK_INTEGER,CALENDAR_MONTH_INTEGER, CALENDAR_MONTH_DESC, END_OF_CAL_MONTH, CALENDAR_QUARTER_DESC, CALENDAR_YEAR FROM times_dim", ses_db_sor)
    
        if not times_tran.empty:
            for id,dname,dinweek,dinmo,caweek,camoin,camode,encamo,caqude,caye \
                in zip(times_tran['TIME_ID'],times_tran['DAY_NAME'],
                times_tran['DAY_INTEGER_IN_WEEK'], times_tran['DAY_INTEGER_IN_MONTH'],
                times_tran['CALENDAR_WEEK_INTEGER'],times_tran['CALENDAR_MONTH_INTEGER'],times_tran['CALENDAR_MONTH_DESC'],
                times_tran['END_OF_CAL_MONTH'],times_tran['CALENDAR_QUARTER_DESC'],times_tran['CALENDAR_YEAR']):
                times_dict_tran["time_id"].append(id)
                times_dict_tran["day_name"].append(dname)
                times_dict_tran["day_integer_in_week"].append(dinweek)
                times_dict_tran["day_integer_in_month"].append(dinmo)
                times_dict_tran["calendar_week_integer"].append(caweek)
                times_dict_tran["calendar_month_integer"].append(camoin)
                times_dict_tran["calendar_month_desc"].append(camode)
                times_dict_tran["end_of_cal_month"].append(encamo)
                times_dict_tran["calendar_month_name"].append(" ")
                times_dict_tran["calendar_quarter_desc"].append(caqude)
                times_dict_tran["calendar_year"].append(caye)
        if not times_sor.empty:
            for id,dname,dinweek,dinmo,caweek,camoin,camode,encamo,caqude,caye \
                in zip(times_sor['TIME_ID'],times_sor['DAY_NAME'],
                times_sor['DAY_INTEGER_IN_WEEK'], times_sor['DAY_INTEGER_IN_MONTH'],
                times_sor['CALENDAR_WEEK_INTEGER'],times_sor['CALENDAR_MONTH_INTEGER'],times_sor['CALENDAR_MONTH_DESC'],
                times_sor['END_OF_CAL_MONTH'],times_sor['CALENDAR_QUARTER_DESC'],times_sor['CALENDAR_YEAR']):
                times_dict_sor["time_id"].append(id)
                times_dict_sor["day_name"].append(dname)
                times_dict_sor["day_integer_in_week"].append(dinweek)
                times_dict_sor["day_integer_in_month"].append(dinmo)
                times_dict_sor["calendar_week_integer"].append(caweek)
                times_dict_sor["calendar_month_integer"].append(camoin)
                times_dict_sor["calendar_month_desc"].append(camode)
                times_dict_sor["end_of_cal_month"].append(encamo)
                times_dict_sor["calendar_month_name"].append(" ")
                times_dict_sor["calendar_quarter_desc"].append(caqude)
                times_dict_sor["calendar_year"].append(caye)
    
        if times_dict_sor["time_id"]:
            df_times_tra = pd.DataFrame(times_dict_tran)
            df_times_sor = pd.DataFrame(times_dict_sor)
            fusion = df_times_tra.merge(df_times_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            fusion.to_sql('times_dim', ses_db_sor, if_exists="append",index=False)
            
        else:
            df_times_tra = pd.DataFrame(times_dict_tran)
            df_times_tra.to_sql('times_dim', ses_db_sor, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass