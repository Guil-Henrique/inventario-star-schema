import requests
import pandas as pd

def get_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"erro: {response.status_code}")
        return None

urls = {
    'ProductInventories': 'https://demodata.grapecity.com/adventureworks/api/v1/ProductInventories',
    'Product': 'https://demodata.grapecity.com/adventureworks/api/v1/Products',
    'Customer': 'https://demodata.grapecity.com/adventureworks/api/v1/Customers',
    'SalesPerson': 'https://demodata.grapecity.com/adventureworks/api/v1/SalesPersons',
    'Store': 'https://demodata.grapecity.com/adventureworks/api/v1/stores'
}

data = {}
#for key, url in urls.items():
#   json_data = get_data(url)
#    if json_data:
#        data[key] = pd.DataFrame(json_data)
#for key, df in data.items():
#   print(f"\n{key}:")
#   print(df.head(5))
