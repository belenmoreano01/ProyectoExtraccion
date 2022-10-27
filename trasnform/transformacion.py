from datetime import datetime


def get_month(mes):
    return datetime.strptime(str(mes),'%m').strftime('%B').upper()

def parce_date (date):
    return datetime.strptime(date,'%Y-%m-d%')

def change_date(datevar):
    fecha_str = datevar
    fecha =  datetime.strptime(fecha_str,'%d-%b-%y')
    return (fecha)

def list_countries(countrie_id, countriesor):
        
        arreglo=dict()
        if not countriesor.empty:
            for id,cou_id \
                in zip(countriesor['ID'],countriesor['COUNTRY_ID']):
                arreglo[cou_id] = id
        return arreglo[countrie_id]

