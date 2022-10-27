import traceback
from extract.extract_channels import ext_channels
from extract.extract_countries import ext_countries
from extract.extract_customers import ext_customers
from extract.extract_products import ext_products
from extract.extract_promotion import ext_promotions
from extract.extract_sales import ext_sales
from extract.extract_times import ext_times
from util.trans_load import loadSor, transform
from trasnform.transformacion import list_countries

try:
    
    transform()
    loadSor()
except:

    traceback.print_exc()

finally:

    pass