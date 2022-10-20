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
channel_conf = confP ['csv']['times_csv']
con_db_stg = Db_Connection(type, host, port, user, pwd, db)


def ext_times():
    try:
   
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception (f"The give database type {type} is not valid")
        elif ses_db_stg == -2:
            raise Exception ("Error trying to connect to the b2b_dwh_staging")

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
            "calendar_year":[]
        }
        times_csv = pd.read_csv(f'csvs/times.csv')
        #Process CSV Content
        if not times_csv.empty:
            for id,dname,dinweek,dinmo,caweek,camoin,camode,encamo,caqude,caye \
                in zip(times_csv['TIME_ID'],times_csv['DAY_NAME'],
                times_csv['DAY_NUMBER_IN_WEEK'], times_csv['DAY_NUMBER_IN_MONTH'],
                times_csv['CALENDAR_WEEK_NUMBER'],times_csv['CALENDAR_MONTH_NUMBER'],times_csv['CALENDAR_MONTH_DESC'],
                times_csv['END_OF_CAL_MONTH'],times_csv['CALENDAR_QUARTER_DESC'],times_csv['CALENDAR_YEAR']):
                times_dict["time_id"].append(id)
                times_dict["day_name"].append(dname)
                times_dict["day_integer_in_week"].append(dinweek)
                times_dict["day_integer_in_month"].append(dinmo)
                times_dict["calendar_week_integer"].append(caweek)
                times_dict["calendar_month_integer"].append(camoin)
                times_dict["calendar_month_desc"].append(camode)
                times_dict["end_of_cal_month"].append(encamo)
                times_dict["calendar_month_name"].append(" ")
                times_dict["calendar_quarter_desc"].append(caqude)
                times_dict["calendar_year"].append(caye)
        if times_dict["time_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE times_ext")
            df_times_ext = pd.DataFrame(times_dict)
            df_times_ext.to_sql('times_ext', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass