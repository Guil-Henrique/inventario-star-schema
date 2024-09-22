import requests
import psycopg2
import json
import time

## a tabela products tem paginação na api, percorri as pg utilizando while pra pegar todos
def get_all_data(url, page_size=100, paginated=True):
    all_data = []
    page_number = 1

    while True:
        if paginated:
            response = requests.get(url, params={'PageNumber': page_number, 'PageSize': page_size})
        else:
            response = requests.get(url)  

        if response.status_code == 200:
            data = response.json()
            if not data:
                break
            all_data.extend(data)
            print(f"log pagina: {page_number}: {len(data)}")
            
            if paginated:
                page_number += 1
            else:
                break 
        else:
            ## print(f"erro: {response.status_code}")
            break

    return all_data

## criaçao das tables
def create_tables(conn):
    with conn.cursor() as cur:

        cur.execute(""" 
                    
            CREATE TABLE IF NOT EXISTS products (
                product_id INT PRIMARY KEY,
                name VARCHAR(255),
                product_number VARCHAR(50),
                safety_stock_level INT,
                reorder_point INT,
                standard_cost DECIMAL,
                list_price DECIMAL,
                category VARCHAR(50),
                subcategory VARCHAR(50)
            );

            CREATE TABLE IF NOT EXISTS locations (
                location_id INT PRIMARY KEY,
                name VARCHAR(255),
                cost_rate DECIMAL
            );

            CREATE TABLE IF NOT EXISTS calendario (
                date DATE PRIMARY KEY,
                ano INT,
                mes INT,
                day INT
            );

            CREATE TABLE IF NOT EXISTS product_inventories (
                product_id INT,
                location_id INT,
                date DATE,
                quantity INT,
                PRIMARY KEY (product_id, location_id, date),
                FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE,
                FOREIGN KEY (location_id) REFERENCES locations(location_id) ON DELETE CASCADE,
                FOREIGN KEY (date) REFERENCES calendario(date) ON DELETE CASCADE
            );


        """)
        conn.commit()

##
def insert_data(conn, table_name, data):
    cursor = conn.cursor()
    if data:
        for item in data:
            if table_name == 'product_inventories':
                date_str = item['modifiedDate'].split('T')[0]
                year, month, day = map(int, date_str.split('-'))

                
                cursor.execute("""
                    INSERT INTO calendario (date, ano, mes, day)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (date) DO NOTHING;
                """, (date_str, year, month, day))

                item = {
                    'product_id': item['productId'],
                    'location_id': item['location']['locationId'],
                    'quantity': item['quantity'],
                    'date': date_str  
                }
            elif table_name == 'products':
                item = {
                    'product_id': item['productId'],
                    'name': item['name'],
                    'product_number': item['productNumber'],
                    'safety_stock_level': item['safetyStockLevel'],
                    'reorder_point': item['reorderPoint'],
                    'standard_cost': item['standardCost'],
                    'list_price': item['listPrice'],
                    'category': item.get('category'),
                    'subcategory': item.get('subcategory')
                }
            elif table_name == 'locations':
                item = {
                    'location_id': item['locationId'],
                    'name': item['name'],
                    'cost_rate': item['costRate']
                }

            keys = item.keys()
            columns = ', '.join(keys)
            values = ', '.join([f"%({k})s" for k in keys])

            if table_name == 'products':
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({values}) ON CONFLICT (product_id) DO NOTHING;"
            elif table_name == 'locations':
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({values}) ON CONFLICT (location_id) DO NOTHING;"
            elif table_name == 'product_inventories':
                query = f"INSERT INTO {table_name} (product_id, location_id, quantity, date) VALUES (%(product_id)s, %(location_id)s, %(quantity)s, %(date)s) ON CONFLICT (product_id, location_id, date) DO NOTHING;"

            try:
                cursor.execute(query, item)
            except Exception as e:
                print(f"erro na tabela: {table_name}: {e}")
                conn.rollback()
                return

        conn.commit()
        print(f"dados INSERIDOS {table_name}")
    else:
        print(f"dados NÃO INSERIDOS {table_name}")

## db connection. POSTGRES 
def connect_db(retries=5):
    conn = None
    while retries > 0:
        try:
            conn = psycopg2.connect(
                dbname="dbinventario",
                user="postgres",
                password="root",
                host="db",
                port="5432"
            )

            return conn
        
        #Tentar reconectar ao banco de dados
        except Exception as e:
            print(f"Erro ao conectar ao banco de dados: {e}")
            retries -= 1
            print(f"Tentando novamente em 5 segundos... ({retries} tentativas restantes)")
            time.sleep(5)

    print("Não foi possível conectar ao banco de dados após várias tentativas.")
    return None

## tabelas utilizadas da api- star schema controle de estoque. apenas products com paginas
urls = {
    'product_inventories': 'https://demodata.grapecity.com/adventureworks/api/v1/ProductInventories',
    'products': 'https://demodata.grapecity.com/adventureworks/api/v1/Products',
    'locations': 'https://demodata.grapecity.com/adventureworks/api/v1/Locations'
}

def main():
    conn = connect_db()
    if conn:
        create_tables(conn) 

        data_products = get_all_data(urls['products'], paginated=True)
        if data_products:
            insert_data(conn, 'products', data_products)


        data_locations = get_all_data(urls['locations'], paginated=False)
        if data_locations:
            print(f"Dados obtidos para locations: {len(data_locations)} registros")
            insert_data(conn, 'locations', data_locations)


        data_product_inventories = get_all_data(urls['product_inventories'], paginated=False)
        if data_product_inventories:
            print(f"Dados obtidos para product_inventories: {len(data_product_inventories)} registros")
            insert_data(conn, 'product_inventories', data_product_inventories)

        conn.close()

if __name__ == "__main__":
    main()

