#-------------------------------------------------
# Imports
from operator import index
import pandas as pd
import pyodbc


#Variaveis uteis
connection_server = "PRD-INST-BI2\BI"  # Informar instancia de conexão
dataBase = "INBOUND"  # Informar database

#Conectando ao banco de dados
cnxn_str = ("Driver={SQL Server Native Client 11.0};"
            "Server=" + connection_server + ";"
            "Database=" + dataBase + ";"
            "Trusted_Connection=yes;")
cnxn = pyodbc.connect(cnxn_str)

#-------------------------------------------------
# Variaveis de query
nome_tabela = "bdt.tb_reservatorioee" # Informar o nome da tabela COM shchema
nome_tabela_pk = "tb_reservatorioee" # Informar o nome da tabela SEM shchema

query_tabela = """SELECT * FROM {0}""".format(nome_tabela)

query_pk = """SELECT column_name FROM information_schema.key_column_usage
              WHERE OBJECTPROPERTY(OBJECT_ID(constraint_name), 'IsPrimaryKey') = 1
               and table_name = '{0}'""".format(nome_tabela_pk)

#-------------------------------------------------
# Executa query no banco
data = pd.read_sql(query_tabela, cnxn, chunksize=1000)
colunas_pk = pd.read_sql(query_pk, cnxn)

#-------------------------------------------------
# Preenche lista com as colunas pk encontradas
pk = []
for row in colunas_pk.itertuples(index=False):
    pk.append(row[0])

#-------------------------------------------------
# Cria e preenche o arquivo JSON para a tabela informada
file = open("{0}.json".format(nome_tabela), 'w')
file.write("[ ")

for df in data:
    k = 0
    for row in df.itertuples(index=False):
        js = ["{", '"Acao": "Inclusao",', '"TipoObjeto": "{0}",'.format(nome_tabela), '"MudancaEstrutura": false,', '"Propriedades": [']
        conteudo_pk = []
        for i in range(len(row)):
            if df.columns[i] in pk:
                conteudo_pk.append(row[i])
            else:
                js.append("{")
                js.append('"Nome": "{0}",'.format(df.columns[i]))
                js.append('"Valor": "{0}"'.format(row[i]))
                if i + 1 == len(row):
                    js.append("}")
                else:
                    js.append("},")

        js.append("],")
        js.append('"PropriedadesChave": [')
        for q in range (len(pk)):
            js.append("{")
            js.append('"Nome": "{0}",'.format(pk[q]))
            js.append('"Valor": "{0}"'.format(conteudo_pk[q]))
            if q + 1 == len(pk):
                js.append("}")
            else:
                js.append("},")
        js.append("]")
        if k == df.index[-1]:
            js.append("}")
        else:
            js.append("},")
        file.write(" ".join(js))
        k += 1
file.write("]")
print("Finalizou com sucesso")