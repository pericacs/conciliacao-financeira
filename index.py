# flake8: noqa

from cartao_conciliacao_credito_avista import conciliar_cav_venda_credito
from Importar_arq_conciliacao_cielo import csv_to_json, importar_json, excel_to_json
from conciliacao_diaria import executar_passos_conciliacao
from enviar_email import enviar_email

# csvFilePath = r'C:/PYTHON/Conciliacao-Cielo/arquivo/dados.csv'
#jsonFilePath = r'C:/PYTHON/Conciliacao-Cielo/arquivo/dados.json'
# excel_to_json(csvFilePath, jsonFilePath)
# csv_to_json(csvFilePath, jsonFilePath)  
#importar_json(jsonFilePath)

executar_passos_conciliacao()

# destinatario = 'carlos.andre@fastconnect.com.br'
# assunto = 'Relatório Excel'
# mensagem = 'Segue em anexo o relatório em formato Excel.'
# arquivo_anexo = "C:\\PYTHON\\Conciliacao-Cielo\\tabela base para conciliação - DIARIA - ANTECIPACAO.xlsx"

# enviar_email(destinatario, assunto, mensagem, arquivo_anexo)