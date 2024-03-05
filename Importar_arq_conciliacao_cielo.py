# flake8: noqa
import psycopg2
import json
import csv
from unidecode import unidecode
from os import write
from datetime import datetime
from dotenv import load_dotenv
import os
import pandas as pd
import json
from unidecode import unidecode 

def importar_json(jsonFilePath):
# Carregar variáveis de ambiente do arquivo .env
    load_dotenv()

    # Acessar as variáveis de ambiente
    dbname = os.getenv('DB_NAME')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')

    # Conectar ao banco de dados usando as variáveis de ambiente
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

    data_atual = datetime.now()
    data_formatada = data_atual.strftime("%m/%d/%Y %H:%M:%S")
    # Dados JSON
    with open(jsonFilePath) as file:
        data = json.load(file)


    # Conectar-se ao banco de dados    
    cursor = conn.cursor()
    
    for row in data:
        # Converte a data para o formato correto (YYYY-MM-DD)
        if row['Data de pagamento'] == '-':
            dt_pagamento = None
        else:
            dt_pagamento = datetime.strptime(row['Data de pagamento'], "%d/%m/%Y").date()
        
        if row['Data do lancamento'] == '-':
                dt_lancamento = None
        else:
            dt_lancamento = datetime.strptime(row['Data do lancamento'], "%d/%m/%Y").date()
        
        if row['Data da autorizacao da venda'] == '-':
            dt_autorizacao_venda = None
        else:
            dt_autorizacao_venda = datetime.strptime(row['Data da autorizacao da venda'], "%d/%m/%Y").date()                
                
        # try:
        #     if row['Data de pagamento']:
        #         dt_pagamento = datetime.strptime(row['Data de pagamento'], "%d/%m/%Y").date()
        #     else:
        #         dt_pagamento = None

        #     if row['Data do lancamento']:
        #         dt_lancamento = datetime.strptime(row['Data do lancamento'], "%d/%m/%Y").date()
        #     else:
        #         dt_lancamento = None

        #     if row['Data da autorizacao da venda']:
        #         dt_autorizacao_venda = datetime.strptime(row['Data da autorizacao da venda'], "%d/%m/%Y").date()
        #     else:
        #         dt_autorizacao_venda = None
        # except ValueError:
        #     dt_pagamento = None
        #     dt_lancamento = None
        #     dt_autorizacao_venda = None
        # Converte os valores monetários para o formato numeric(10,2)
        row['Valor bruto'] = converter_valor_monetario(row['Valor bruto'])
        row['Valor descontado'] = converter_valor_monetario(row['Valor descontado'])
        row['Valor liquido'] = converter_valor_monetario(row['Valor liquido'])
        row['Valor cobrado'] = converter_valor_monetario(row['Valor cobrado'])
        row['Valor pendente'] = converter_valor_monetario(row['Valor pendente'])
        row['Valor total'] = converter_valor_monetario(row['Valor total'])      
        row['Taxas (%)'] = converter_valor_monetario(row['Taxas (%)'])        

        insert_query = '''
        INSERT INTO webhook_cielo_table_log 
        (
            dt_conciliacao,
	        tp_lancamento,	
	        nm_banco,
	        nu_agencia,
	        nu_conta,
	        tp_gravame,
            dt_pagamento,
            dt_lancamento,
            dt_autorizacao_venda,
            nm_bandeira,
            nm_forma_pagamento,
            nu_parcela,
            nu_qtde_parcela,
            nu_cartao,
            cd_transacao,
            ds_tid,
            ds_arp,
            ds_nsu,
            vl_bruto,
            vl_desconto,
            vl_liquido,
            vl_cobrado,
            vl_pendente,
            vl_total,
            ds_canal_venda,
            tp_captura,
            ds_resumo_operacao,
            nu_taxa,
            cd_venda,
            nu_operacao,
            nu_pedido
        )
        VALUES (  
            %s, -- dt_conciliacao
            %s, -- Tipo de Lancamento
            %s, -- Banco
            %s, -- Agencia
            %s, -- Conta
            %s, -- Gravame
            %s, -- Data de pagamento
            %s, -- Data do lancamento
            %s, -- Data da autorizacao da venda
            %s, -- Bandeira
            %s, -- Forma de Pagamento
            %s, -- Numero da parcela
            %s, -- Quantidade de parcelas
            %s, -- Numero do cartao
            %s, -- Codigo da transacao
            %s, -- TID
            %s, -- Codigo de autorizacao
            %s, -- NSU
            %s, -- Valor bruto
            %s, -- Valor descontado
            %s, -- Valor liquido
            %s, -- Valor cobrado
            %s, -- Valor pendente
            %s, -- Valor total
            %s, -- Canal de venda
            %s, -- Tipo de captura
            %s, -- Resumo da operacao
            %s, -- Taxas
            %s, -- Codigo da venda
            %s, -- Numero da operacao
            %s  -- Numero do pedido     
        );
        '''
        # Montando a tupla de valores
        values = (
            data_formatada,
            row['Tipo de Lancamento'],             
            row['Banco'], 
            row['Agencia'], 
            row['Conta'], 
            row['Gravame'],            
            dt_pagamento, 
            dt_lancamento, 
            dt_autorizacao_venda, 
            row['Bandeira'],
            row['Forma de Pagamento'], 
            row['Numero da parcela'], 
            row['Quantidade de parcelas'], 
            row['Numero do cartao'],
            row['Codigo da transacao'], 
            row['TID'], 
            row['Codigo de autorizacao'], 
            row['NSU'], 
            row['Valor bruto'], 
            row['Valor descontado'],
            row['Valor liquido'], 
            row['Valor cobrado'], 
            row['Valor pendente'], 
            row['Valor total'], 
            row['Canal de venda'], 
            row['Tipo de captura'], 
            row['Resumo da operacao'], 
            row['Taxas (%)'], 
            row['Codigo da venda'], 
            row['Numero da operacao'],
            row['Numero do pedido']        
        )

        cursor.execute(insert_query, values)
        conn.commit()

    # Fechar a conexão com o banco de dados
    cursor.close()
    conn.close()


def converter_valor_monetario(valor_string):
     # Remover todos os pontos de milhar
    valor_string = valor_string.replace('.', '')    
    # Remover o símbolo 'R$' e substituir ',' por '.' 
    valor_string = valor_string.replace('R$', '').replace(',', '.')
    
    # Remover todos os espaços em branco antes de tentar converter para float
    valor_string = valor_string.strip().replace(' ', '')
    
    # Se o valor estiver vazio, retornar zero
    if valor_string == '':
        return 0.0
    
    # Se o valor contiver apenas um sinal de negativo, retornar zero
    if valor_string == '-':
        return 0.0
    
    # Se não for vazio, converter para float
    valor_float = float(valor_string)
    
    return valor_float
    

def csv_to_json(csvFilePath, jsonFilePath):
    jsonArray = []
      
    #read csv file
    with open(csvFilePath, encoding='utf-8-sig') as csvf: 
        #load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvf, delimiter=';') 
# Converter os dados para uma lista de dicionários
        data = list(csvReader)


        #convert each csv row into python dict
        for row in data: 
            row = {unidecode(key): unidecode(value) for key, value in row.items()}

            #add this python dict to json array
            jsonArray.append(row)
  
    #convert python jsonArray to JSON String and write to file
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf: 
        jsonString = json.dumps(jsonArray, indent=4)
        jsonf.write(jsonString)



def excel_to_json(excelFilePath, jsonFilePath):
    jsonArray = []

    # Carregar o arquivo Excel
    df = pd.read_excel(excelFilePath, skiprows=4)  # Começa a ler a partir da 5ª linha

    # Converter o DataFrame para uma lista de dicionários
    data = df.to_dict(orient='records')

    # Converter caracteres acentuados para ASCII
    data = [{unidecode(key): unidecode(value) for key, value in row.items()} for row in data]

    # Adicionar os dicionários à lista jsonArray
    jsonArray.extend(data)

    # Converter o jsonArray para uma string JSON e escrever no arquivo
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf:
        jsonString = json.dumps(jsonArray, indent=4)
        jsonf.write(jsonString)