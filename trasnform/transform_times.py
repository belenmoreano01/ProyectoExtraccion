from util.db_connection import Db_Connection
from util.properties import confStg
import pandas as pd
import traceback

from trasnform.transformacion import change_date

def tran_times(map_id):
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
        times_dict = {
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
            "calendar_year":[],
            "procesoetl_id":[]
        }
        times_ext = pd.read_sql("SELECT TIME_ID, DAY_NAME, DAY_INTEGER_IN_WEEK, DAY_INTEGER_IN_MONTH,CALENDAR_WEEK_INTEGER,CALENDAR_MONTH_INTEGER, CALENDAR_MONTH_DESC, END_OF_CAL_MONTH, CALENDAR_QUARTER_DESC, CALENDAR_YEAR FROM times_ext", ses_db_stg)

        if not times_ext.empty:
            for id,dname,dinweek,dinmo,caweek,camoin,camode,encamo,caqude,caye \
                in zip(times_ext['TIME_ID'],times_ext['DAY_NAME'],
                times_ext['DAY_INTEGER_IN_WEEK'], times_ext['DAY_INTEGER_IN_MONTH'],
                times_ext['CALENDAR_WEEK_INTEGER'],times_ext['CALENDAR_MONTH_INTEGER'],times_ext['CALENDAR_MONTH_DESC'],
                times_ext['END_OF_CAL_MONTH'],times_ext['CALENDAR_QUARTER_DESC'],times_ext['CALENDAR_YEAR']):
                times_dict["time_id"].append(change_date(id))
                times_dict["day_name"].append(dname)
                times_dict["day_integer_in_week"].append(dinweek)
                times_dict["day_integer_in_month"].append(dinmo)
                times_dict["calendar_week_integer"].append(caweek)
                times_dict["calendar_month_integer"].append(camoin)
                times_dict["calendar_month_desc"].append(camode)
                times_dict["end_of_cal_month"].append(change_date(encamo))
                times_dict["calendar_month_name"].append(" ")
                times_dict["calendar_quarter_desc"].append(caqude)
                times_dict["calendar_year"].append(caye)
                times_dict["procesoetl_id"].append(map_id)
        if times_dict["time_id"]:
            df_times_tran = pd.DataFrame(times_dict)
            df_times_tran.to_sql('times', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass