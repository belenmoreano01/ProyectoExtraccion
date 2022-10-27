from logging import exception
from util.db_connection import Db_Connection
from util.properties import confStg
import traceback


def etl_version():
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
        register=ses_db_stg.execute('INSERT INTO  PORCES_ETL values ()')
        revetl=ses_db_stg.execute('SELECT PROCESO_ID  FROM PORCES_ETL ORDER BY PROCESO_ID DESC LIMIT 1').scalar()
        print(revetl)
        return revetl
    except:
        traceback.print_exc()
    finally:
        pass

