from jproperties import Properties


def confStg():
    configs = Properties()
    with open('properties\stg.properties', 'rb') as confStg_file:
        configs.load(confStg_file)
    confStg={
        "TYPE": configs.get("type").data,
        "HOST":configs.get("host").data,
        'PORT':configs.get("port").data,
        'USER':configs.get("user").data,
        'PASSWORD':configs.get("pwd").data,
        'DATABASE':configs.get("schema").data  
    }
    return confStg


def confSor():
    configs = Properties()
    with open('properties\sor.properties', 'rb') as confSor_file:
        configs.load(confSor_file)
    confSor={
        'TYPE': configs.get("type").data,
        'HOST':configs.get("host").data,
        'PORT':configs.get("port").data,
        'USER':configs.get("user").data,
        'PASSWORD':configs.get("pwd").data,
        'DATABASE':configs.get("schema").data  
    }
    return confSor


configs = Properties()
with open('properties\csvs.properties', 'rb') as data_file:
    configs.load(data_file)

def path():
        data_path=configs.get("PATH").data
        return data_path

