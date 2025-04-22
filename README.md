
# 🛠️ Projeto de Engenharia de Dados com Python – Pipeline ETL

Este repositório contém um projeto prático desenvolvido durante o curso de Engenharia de Dados da [Alura](https://www.alura.com.br/), com foco na construção de um **pipeline de dados completo utilizando Python**. O objetivo principal foi aplicar os conceitos de **ETL (Extract, Transform, Load)** em um fluxo automatizado de dados, reforçando o uso de boas práticas e ferramentas como o GitHub.

---

## 📌 Objetivos do Projeto

- Compreender o conceito de **pipeline de dados** e sua importância em projetos de Ciência e Engenharia de Dados.
- Construir um pipeline ETL funcional utilizando Python.
- Realizar **extração, transformação e carga** de dados, integrando com o banco de dados **MongoDB** e também com **MySQL**.
- Praticar a colaboração e versionamento com o **GitHub**.
- Aplicar testes e validações básicas para garantir a confiabilidade dos dados.

---

## 🧱 Estrutura do Projeto

Este projeto foi implementado de duas formas:  
- via **Jupyter Notebooks**, para experimentação passo a passo, e  
- via **scripts `.py`**, para execução automatizada do pipeline.

### 🔹 Notebooks

1. **`extract_and_save_data.ipynb`**
   - Extrai dados de uma API pública.
   - Armazena os dados em arquivos locais.

2. **`transform_data.ipynb`**
   - Realiza limpeza, normalização e tratamento dos dados usando Pandas.
   - Aplica filtros e cria arquivos `.csv` organizados.

3. **`save_data_mysql.ipynb`**
   - Conecta-se ao MySQL e carrega os dados já transformados.

### 🔹 Scripts Python

- **`extract_and_save_data.py`**  
  Conecta-se ao MongoDB, extrai dados de uma API (`https://labdados.com/produtos`) e insere os documentos em uma coleção.

- **`transform_data.py`**  
  Aplica renomeação de colunas, filtros por categoria ou data com expressões regulares, e formata datas. Exporta os dados como arquivos CSV.

---

## 🧪 Validação e Boas Práticas

- Verificação de colunas antes de transformações críticas.
- Conversão de datas para formato padrão.
- Impressões de debug para facilitar o rastreio dos dados.

---

## 🛠️ Tecnologias Utilizadas

- Python
- Jupyter Notebook
- Pandas
- Requests
- PyMongo (MongoDB)
- PyMySQL (MySQL)
- Git & GitHub

---

## 📚 Aprendizados

Durante o desenvolvimento deste projeto, os seguintes tópicos foram estudados e aplicados:

- Implementação de pipelines de dados do tipo ETL.
- Conexão e manipulação de dados em bancos MongoDB e MySQL.
- Uso de bibliotecas para transformação de dados.
- Criação de scripts reutilizáveis em Python.
- Testes e boas práticas em scripts de dados.
- Organização de projetos com GitHub.

