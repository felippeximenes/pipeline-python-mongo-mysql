import requests
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

def connect_mongo(uri):
    """
    Estabelece conexão com o MongoDB usando a URI fornecida.
    
    Parâmetros:
    uri (str): String de conexão do MongoDB.

    Retorna:
    MongoClient: Objeto cliente do MongoDB para interação com o banco de dados.
    """
    try:
        client = MongoClient(uri, server_api=ServerApi('1'))
        client.admin.command('ping')  # Testa a conexão
        print("✅ Conectado ao MongoDB com sucesso!")
        return client
    except Exception as e:
        print(f"❌ Erro ao conectar ao MongoDB: {e}")
        return None

def create_connect_db(client, db_name):
    """
    Cria e conecta-se a um banco de dados no MongoDB.

    Parâmetros:
    client (MongoClient): Cliente MongoDB conectado.
    db_name (str): Nome do banco de dados.

    Retorna:
    Database: Objeto do banco de dados.
    """
    db = client[db_name]
    print(f"📂 Conectado ao banco de dados: {db_name}")
    return db

def create_connect_collection(db, col_name):
    """
    Cria e conecta-se a uma coleção dentro de um banco de dados.

    Parâmetros:
    db (Database): Objeto do banco de dados MongoDB.
    col_name (str): Nome da coleção.

    Retorna:
    Collection: Objeto da coleção para interação com documentos.
    """
    collection = db[col_name]
    print(f"📁 Conectado à coleção: {col_name}")
    return collection

def extract_api_data(url):
    """
    Extrai dados de uma API e retorna em formato JSON.

    Parâmetros:
    url (str): URL da API de onde os dados serão extraídos.

    Retorna:
    dict: Dados extraídos da API em formato JSON.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Lança erro se a requisição falhar
        print("✅ Dados extraídos da API com sucesso!")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao extrair dados da API: {e}")
        return None

def insert_data(col, data):
    """
    Insere os dados extraídos na coleção MongoDB.

    Parâmetros:
    col (Collection): Objeto da coleção onde os dados serão inseridos.
    data (list ou dict): Dados a serem inseridos.

    Retorna:
    int: Quantidade de documentos inseridos.
    """
    if isinstance(data, dict):  # Se for um dicionário, transforma em lista
        data = [data]

    if data:
        result = col.insert_many(data)  # Insere os documentos na coleção
        print(f"✅ {len(result.inserted_ids)} documentos inseridos na coleção!")
        return len(result.inserted_ids)
    else:
        print("⚠️ Nenhum dado disponível para inserção.")
        return 0

if __name__ == "__main__":
    # Definição de parâmetros
    URI = "mongodb+srv://felippelpximenes:vzGsH8pudX92ozUh@cluster-pipeline.pzicdbg.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-Pipeline"  
    DB_NAME = "db_novo"
    COLLECTION_NAME = "produtos"
    API_URL = "https://labdados.com/produtos"

    # Fluxo de execução
    client = connect_mongo(URI)
    if client:
        db = create_connect_db(client, DB_NAME)
        collection = create_connect_collection(db, COLLECTION_NAME)
        api_data = extract_api_data(API_URL)
        if api_data:
            insert_data(collection, api_data)
