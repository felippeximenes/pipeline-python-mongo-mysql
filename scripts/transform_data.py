from extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection
import pandas as pd


# Funcao para percorrer todos os documentos da colecao (como se fossem linhas de uma plnanilha) e os imprime na tela
def visualize_collection(col):
    for doc in col.find():
        print(doc)

#Renomeando coluna dentro do banco de dados
def rename_column(col, col_name, new_name):
    col.update_many({}, {"$rename": {f"{col_name}": f"{new_name}"}})

#Buscando todos os produtos em uma determinada categoria
def select_category(col, category):
    query = { "Categoria do Produto": f"{category}"}
    
    lista_categoria = []
    for doc in col.find(query):
        lista_categoria.append(doc)

    return lista_categoria

#Seleciona documentos que correspondam a uma expressao regular especifica
def make_regex(col, regex):
    query = {"Data da Compra": {"$regex": f"{regex}"}}

    lista_regex = []
    for doc in col.find(query):
        lista_regex.append(doc)
    
    return lista_regex

# Criando um dataframe (tabela organizada) a partir de uma lista de documentos
def create_dataframe(lista):
    df =  pd.DataFrame(lista)
    return df

#Convertando as datas de um formato para outro (ano-mes-dia)
def format_date(df):
    print("Colunas do DataFrame:", df.columns)  # Adiciona esta linha para debug
    
    if "Data da Compra" not in df.columns:
        print("Erro: A coluna 'Data da Compra' não existe no DataFrame!")
        return  # Sai da função sem tentar formatar
    
    df["Data da Compra"] = pd.to_datetime(df["Data da Compra"], format="%d/%m/%Y")
    df["Data da Compra"] = df["Data da Compra"].dt.strftime("%Y-%m-%d")


#Salvando o dataframe em um arquivo CSV
def save_csv(df, path):
    df.to_csv(path, index=False)
    print(f"\nO arquivo {path} foi salvo")

if __name__ == "__main__":

    # estabelecendo a conexão e recuperando os dados do MongoDB
    client = connect_mongo("mongodb+srv://felippelpximenes:vzGsH8pudX92ozUh@cluster-pipeline.pzicdbg.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-Pipeline")
    db = create_connect_db(client, "db_produtos_desafio")
    col = create_connect_collection(db, "produtos")

    # renomeando as colunas de latitude e longitude
    rename_column(col, "lat", "Latitude")
    rename_column(col, "lon", "Longitude")

    # salvando os dados da categoria livros
    lst_livros = select_category(col, "livros")
    df_livros = create_dataframe(lst_livros)
    format_date(df_livros)
    save_csv(df_livros, "../data_teste/tb_livros.csv")

    # salvando os dados dos produtos vendidos a partir de 2021
    lst_produtos = make_regex(col, "/202[1-9]")
    df_produtos = create_dataframe(lst_produtos)
    format_date(df_produtos)
    save_csv(df_produtos, "../data_teste/tb_produtos.csv")