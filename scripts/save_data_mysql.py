import mysql.connector
import pandas as pd

import mysql.connector
import pandas as pd

# Estabelecendo a conexão com o banco de dados MySQL, utlizando os dados do host, usario e senha
# A função deve retornar a conexão estabalecida
def connect_mysql(host_name, user_name, pw):
    cnx = mysql.connector.connect(
        host = "localhost",
        user = "FELIPPEXIMENES",
        password = "12345"
    )
    print(cnx)
    return cnx

# Criando um cursor para executar comandos SQL
# O cursor é um objeto que permite executar comandos SQL e recuperar resultados
def create_cursor(cnx):
    cursor = cnx.cursor()
    return cursor

# Criando um banco de dados
# O banco de dados é criado utilizando o comando SQL CREATE DATABASE
# O nome do banco de dados é passado como parametro
# A função deve retornar o banco de dados criado
def create_database(cursor, db_name):
    cursor.execute(f"CREATE DATABASE {db_name}")
    print(f"\nBase de dados {db_name} criada")

# Listando todos os bancos de dados existentes
# O comando SQL SHOW DATABASES é utilizado para listar todos os bancos de dados existentes
# A função deve retornar todos os bancos de dados existentes
def show_databases(cursor):
    cursor.execute("SHOW DATABASES")
    for x in cursor:
        print(x)

# Criando uma tabela no banco de dados
# A tabela é criada utilizando o comando SQL CREATE TABLE
# O nome da tabela e o nome do banco de dados são passados como parametros
# A função deve retornar a tabela criada
# A tabela possui as seguintes colunas:
# id, Produto, Categoria_Produto, Preco, Frete, Data_Compra, Vendedor, Local_Compra, Avaliacao_Compra, Tipo_Pagamento, Qntd_Parcelas, Latitude e Longitude
# A coluna id é a chave primaria da tabela
# A chave primaria é uma coluna que identifica de forma unica cada linha da tabela
# A chave primaria é criada utilizando o comando SQL PRIMARY KEY
# A tabela deve ter as colunas qye correspondam aos dados que serao inseridos posteriormente
def create_product_table(cursor, db_name, tb_name):    
    cursor.execute(f"""
        CREATE TABLE {db_name}.{tb_name}(
                id VARCHAR(100),
                Produto VARCHAR(100),
                Categoria_Produto VARCHAR(100),
                Preco FLOAT(10,2),
                Frete FLOAT(10,2),
                Data_Compra DATE,
                Vendedor VARCHAR(100),
                Local_Compra VARCHAR(100),
                Avaliacao_Compra INT,
                Tipo_Pagamento VARCHAR(100),
                Qntd_Parcelas INT,
                Latitude FLOAT(10,2),
                Longitude FLOAT(10,2),
                
                PRIMARY KEY (id));
    """)
                   
    print(f"\nTabela {tb_name} criada")

# Listando todas as tabelas existentes no banco de dados
# O comando SQL SHOW TABLES é utilizado para listar todas as tabelas existentes
# A função deve retornar todas as tabelas existentes
def show_tables(cursor, db_name):
    cursor.execute(f"USE {db_name}")
    cursor.execute("SHOW TABLES")
    for x in cursor:
        print(x)

# Lendo um arquivo CSV e retornando um dataframe do pandas com esses dados
def read_csv(path):
    df = pd.read_csv(path)
    return df

# Adicionando os dados do dataframe na tabela criada
# O comando SQL INSERT INTO é utilizado para adicionar os dados do dataframe na tabela
# A função deve retornar os dados adicionados
def add_product_data(cnx, cursor, df, db_name, tb_name):
    lista = [tuple(row) for _, row in df.iterrows()]
    sql = f"INSERT INTO {db_name}.{tb_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    cursor.executemany(sql, lista)
    print(f"\n {cursor.rowcount} dados foram inseridos na tabela {tb_name}.")
    cnx.commit()

if __name__ == "__main__":
    
    # realizando a conexão com mysql
    cnx = connect_mysql("localhost", "FELIPPEXIMENES", "12345")
    cursor = create_cursor(cnx)

    # criando a base de dados
    create_database(cursor, "db_produtos_teste")
    show_databases(cursor)

    # criando tabela
    create_product_table(cursor, "db_produtos_teste", "tb_livros")
    show_tables(cursor, "db_produtos_teste")

    # lendo e adicionando os dados
    df = read_csv("../data_teste/tbl_livros.csv")
    add_product_data(cnx, cursor, df, "db_produtos_teste", "tb_livros")
