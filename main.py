import requests
import psycopg2
import json
from psycopg2 import sql
import time

def get_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao obter dados da API: {response.status_code}")
        return None

def create_db_and_tables(conn):
    cursor = conn.cursor()

    #crirar as tables
    queries = {
        'ProductInventories': '''
            CREATE TABLE IF NOT EXISTS ProductInventories (
                id SERIAL PRIMARY KEY,
                product_id INT,
                location_id INT,
                shelf VARCHAR(50),
                bin INT,
                quantity INT,
                modified_date TIMESTAMP
            );
        ''',
        'Product': '''
            CREATE TABLE IF NOT EXISTS Product (
                id SERIAL PRIMARY KEY,
                product_id INT,
                name VARCHAR(255),
                product_number VARCHAR(50),
                is_manufactured BOOLEAN,
                is_saleable BOOLEAN,
                color VARCHAR(50),
                safety_stock_level INT,
                reorder_point INT,
                standard_cost DECIMAL,
                list_price DECIMAL,
                size VARCHAR(50),
                size_unit VARCHAR(50),
                weight_unit VARCHAR(50),
                weight DECIMAL,
                days_to_manufacture INT,
                product_line VARCHAR(50),
                class VARCHAR(50),
                style VARCHAR(50),
                subcategory VARCHAR(50),
                category VARCHAR(50),
                model VARCHAR(50),
                sell_start_date TIMESTAMP,
                sell_end_date TIMESTAMP,
                discontinued_date TIMESTAMP,
                modified_date TIMESTAMP
            );
        ''',
        'Customer': '''
            CREATE TABLE IF NOT EXISTS Customer (
                id SERIAL PRIMARY KEY,
                customer_id INT,
                person_id INT,
                store_id INT,
                territory VARCHAR(255),
                account_number VARCHAR(50),
                modified_date TIMESTAMP
            );
        ''',
        'SalesPerson': '''
            CREATE TABLE IF NOT EXISTS SalesPerson (
                id SERIAL PRIMARY KEY,
                sales_person_id INT,
                territory VARCHAR(255),
                sales_quota DECIMAL,
                bonus DECIMAL,
                percent_commission DECIMAL,
                sales_ytd DECIMAL,
                sales_last_year DECIMAL,
                modified_date TIMESTAMP
            );
        ''',
        'Store': '''
            CREATE TABLE IF NOT EXISTS Store (
                id SERIAL PRIMARY KEY,
                store_id INT,
                name VARCHAR(255),
                sales_person_id INT,
                demographics JSONB,
                modified_date TIMESTAMP
            );
        '''
    }

    for table, query in queries.items():
        cursor.execute(query)
        print(f"Tabela {table} verificada/criada com sucesso.")

    conn.commit()

def insert_data(conn, table_name, data):
    cursor = conn.cursor()

    if data:
        for item in data:
            if table_name == 'Store' and 'demographics' in item:
                item['demographics'] = json.dumps(item['demographics'])
            
            keys = item.keys()
            columns = ', '.join(keys)
            values = ', '.join([f"%({k})s" for k in keys])
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            try:
                cursor.execute(query, item)
            except Exception as e:
                print(f"Erro ao inserir dados na tabela {table_name}: {e}")
                conn.rollback() 
                return
        conn.commit()
        print(f"Dados inseridos com sucesso na tabela {table_name}")
    else:
        print(f"Nenhum dado para inserir na tabela {table_name}")

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
            print("Conectado ao banco de dados com sucesso!")
            return conn
        except Exception as e:
            print(f"Erro ao conectar ao banco de dados: {e}")
            retries -= 1
            print(f"Tentando novamente em 5 segundos... ({retries} tentativas restantes)")
            time.sleep(5)

    print("Não foi possível conectar ao banco de dados após várias tentativas.")
    return None

urls = {
    'ProductInventories': 'https://demodata.grapecity.com/adventureworks/api/v1/ProductInventories',
    'Product': 'https://demodata.grapecity.com/adventureworks/api/v1/Products',
    'Customer': 'https://demodata.grapecity.com/adventureworks/api/v1/Customers',
    'SalesPerson': 'https://demodata.grapecity.com/adventureworks/api/v1/SalesPersons',
    'Store': 'https://demodata.grapecity.com/adventureworks/api/v1/stores'
}

def main():
    conn = connect_db()
    if conn:
        create_db_and_tables(conn)

        for table_name, url in urls.items():
            data = get_data(url)
            if data:
                print(f"Dados obtidos para {table_name}: {data[:5]}") 
                insert_data(conn, table_name, data)

        conn.close()

if __name__ == "__main__":
    main()
