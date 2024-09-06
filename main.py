import requests
import psycopg2

def get_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao obter dados da API: {response.status_code}")
        return None

def insert_data(conn, table_name, data):
    cursor = conn.cursor()
    
    if data:
        for item in data:
            keys = item.keys()
            columns = ', '.join(keys)
            values = ', '.join([f"%({k})s" for k in keys])
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            cursor.execute(query, item)
        conn.commit()
        print(f"Dados inseridos com sucesso na tabela {table_name}")
    else:
        print(f"Nenhum dado para inserir na tabela {table_name}")

def connect_db():
    try:
        conn = psycopg2.connect(
            dbname="your_db_name",
            user="your_db_user",
            password="your_db_password",
            host="your_db_host",
            port="your_db_port"
        )
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
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
        for table_name, url in urls.items():
            data = get_data(url)
            if data:
                insert_data(conn, table_name, data)
        conn.close()
if __name__ == "__main__":
    main()
