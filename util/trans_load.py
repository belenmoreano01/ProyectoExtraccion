from load.load_channels import load_channels
from load.load_countries import load_countries
from load.load_customers import load_customers
from load.load_products import load_products
from load.load_promotions import load_promotions
from load.load_sales import load_sales
from load.load_times import load_times
from trasnform.transform_channels import tran_channels
from trasnform.transform_countries import tran_countries
from trasnform.transform_customers import tran_customers
from trasnform.transform_products import tran_products
from trasnform.transform_promotions import tran_promotions
from trasnform.transform_sales import tran_sales
from trasnform.transform_times import tran_times
from util.ETL import etl_version

map_id=etl_version()
def transform():
        
        tran_channels(map_id)
        tran_countries(map_id)
        tran_times(map_id)
        tran_customers(map_id)
        tran_products(map_id)
        tran_promotions(map_id)
        tran_sales(map_id)
    
def loadSor():
        load_channels(map_id)
        load_countries(map_id)
        load_customers(map_id)
        load_products(map_id)
        load_promotions(map_id)
        #load_sales(map_id)
        load_times(map_id)