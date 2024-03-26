# flake8: noqa

from cartao_conciliacao_credito_avista import conciliar_cav_venda_credito
from Importar_arq_conciliacao_cielo import csv_to_json, importar_json, excel_to_json
from conciliacao_diaria import executar_passos_conciliacao
from enviar_email import enviar_email

# csvFilePath = r'C:/PYTHON/conciliacao-financeira/arquivo/dados.csv'
# jsonFilePath = r'C:/PYTHON/conciliacao-financeira/arquivo/dados.json'
# excel_to_json(csvFilePath, jsonFilePath)
# csv_to_json(csvFilePath, jsonFilePath)  
# importar_json(jsonFilePath)

executar_passos_conciliacao()
