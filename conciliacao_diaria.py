# flake8: noqa
import psycopg2
from dotenv import load_dotenv
import os
import pandas as pd
from enviar_email import enviar_email

load_dotenv()

# excluir arquivos temporários
def excluir_arquivo(caminho_arquivo):
    # Tente excluir o arquivo
    try:
        os.remove(caminho_arquivo)
        print(f"O arquivo '{caminho_arquivo}' foi excluído com sucesso.")
    except OSError as e:
        print(f"Erro ao excluir o arquivo: {e}")

# Acessar as variáveis de ambiente
def conexao():
    load_dotenv()
    dbname = os.getenv('DB_NAME')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    srv2 = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
    return srv2 

def executar_passos_conciliacao():
    
    step1 = conciliacao_boletos_arbi()
    step2 = conciliar_boletos_migrados()
    step3 = conciliar_boletos_ccb()
    step4 = conciliar_boletos_titulos_d1()
    step5 = conciliar_titulos()
    
    destinatario = ["carlos.andre@fastconnect.com.br", "lupssouza@gmail.com", "juridico@fastconnect.com.br", "joice.rosario@alibin.com.br"] 
    # destinatario = "carlos.andre@fastconnect.com.br, pericacs@gmail.com" 
    relatorio_diario_antecipacao()    
    assunto = 'Relatório de Conciliação das Antecipações Diárias'
    mensagem = 'Segue em anexo o relatório de conciliação diária das antecipações em formato Excel.'
    arquivo_anexo = "C:\\PYTHON\\conciliacao-financeira\\arquivo\\Conciliacao_Diaria_Antecipacao.xlsx"
    enviar_email(destinatario, assunto, mensagem, arquivo_anexo)
    excluir_arquivo(arquivo_anexo)

    relatorio_diario()
    assunto = 'Relatório de Conciliação Diária'
    mensagem = 'Segue em anexo o relatório de conciliação diária em formato Excel.'
    arquivo_anexo = "C:\\PYTHON\\conciliacao-financeira\\arquivo\\Conciliacao_Diaria.xlsx"
    enviar_email(destinatario, assunto, mensagem, arquivo_anexo)
    excluir_arquivo(arquivo_anexo)


    # step1 = 'step2'
    # step2 = 'step2'
    # step3 = 'step2'
    # step4 = 'step2'
    # step5 = 'step2'
    

    assunto = 'Relatório :: Resultado das Funções'
    mensagem = f"""
    
                    conciliacao_boletos_arbi :: "{step1}" 
                    
                    conciliar_boletos_migrados :: "{step2}"
                    
                    conciliar_boletos_ccb :: "{step3}"
                    
                    conciliar_boletos_titulos_d1 :: "{step4}"
                    
                    conciliar_titulos :: "{step5}"

                """
    destinatario = "carlos.andre@fastconnect.com.br, pericacs@gmail.com"
    arquivo_anexo = ''
    enviar_email(destinatario, assunto, mensagem, arquivo_anexo)  



"""
    As operações aqui não realizadas na Base de dados :: db_pagamentos.
    É feita a verificação do arquivo enviado pelo banco arbi via webhook.     
    Essa função serve para pegar as informações e trata-las para que sejam usadas 
    na validação dos titulos que são pagos e que devem ser repassados para o cliente.
"""    
def conciliacao_boletos_arbi():
    # Acessar as variáveis de ambiente
    dbname_pag = os.getenv('DB_PAG_NAME')
    user_pag = os.getenv('DB_PAG_USER')
    password_pag = os.getenv('DB_PAG_PASSWORD')
    host_pag = os.getenv('DB_PAG_HOST')
    port_pag = os.getenv('DB_PAG_PORT')

    # Conectar ao banco de dados usando as variáveis de ambiente
    srv1 = psycopg2.connect(
        dbname=dbname_pag,
        user=user_pag,
        password=password_pag,
        host=host_pag,
        port=port_pag
    )    

    cur = srv1.cursor()
    cur.execute("select func_conciliar_webhook_arbi_table()")
    srv1.commit()
    result = cur.fetchone()    
    print(result)
    cur.close()
    srv1.close()    
    return result

"""
    depois de limpa as informações enviadas pelo banco arbi ao banco de dados :: db_pagamentos,
    trazemos os dados para a base de dados :: fpay.
"""
def conciliar_boletos_migrados():
    conn = conexao()
    cur = conn.cursor()
    cur.execute("select transferir_dados_conciliacao_arbi_boletos()")    
    conn.commit()
    result = cur.fetchone()
    print(result)
    cur.close()
    conn.close()
    return result

# função para identifica quais titulos foram antecipados e ajusta seus valores para repasse
def conciliar_boletos_ccb():
    conn = conexao()
    cur = conn.cursor()
    cur.execute("select func_conciliar_ccb()")
    conn.commit()
    result = cur.fetchone()
    print(result)
    cur.close()
    conn.close()
    return result

# função para pegar os títulos dos boletos do dia e adicionar 1 dia a mais para o repasse aos clientes
def conciliar_boletos_titulos_d1():
    conn = conexao()
    cur = conn.cursor()
    cur.execute("select conciliar_titulos_d1_arbi()")
    conn.commit()
    result = cur.fetchone()
    print(result)
    cur.close()
    conn.close()
    return result

#  concilia os titulos diários, atualizando conforme os arquivos recebidos dos bancos e adquirentes de cartão
def conciliar_titulos():
    conn = conexao()
    cur = conn.cursor()
    cur.execute("select func_conciliar()")    
    conn.commit()
    result = cur.fetchone()
    print(result)
    cur.close()
    conn.close()
    return result

def relatorio_diario_antecipacao():
    conn = conexao()
    sql  = """
        select 
            dr.id_venda as venda,
            coalesce(id_cliente_split, id_ec) as id_cliente,
            coalesce(nm_cliente_split, nm_ec) as cliente,
            dr.ds_tid || (case when nu_parcela_total = 1 then 0 else dr.nu_parcela end) as tid,
            dr.ds_arp as arp,
            dr.ds_nsu as nsu,
            to_char(dr.dt_venda, 'DD/MM/YYYY') as dt_venda,
            to_char(dt_prevista, 'DD/MM/YYYY') as dt_prevista ,
            dr.ds_taxa,
            dr.fc_vl_recebimento_liquido as vl_repasse,
            dr.vl_repasse as vl_repasse_com_antecipacao,
            a.nm_adquirente 
        from dw_recebiveis dr 
        join parcela p on dr.id_parcela = p.id_parcela
        join adquirente a on a.id_adquirente = p.id_adquirente 
        where             
            coalesce (dr.dt_recebimento, dr.dt_prevista) between date_trunc('day', current_date) and cast(to_char(current_date, 'YYYY-MM-DD') ||' 23:59:59' as timestamp)	
            and id_tipo_antecipacao is not null
            and (tp_recorrencia not like 'R' or tp_recorrencia is null)
            and nm_tipo_pagamento not like 'BOLETO'
        order by cliente asc, dt_venda asc, nm_tipo_pagamento asc
    """

    cursor = conn.cursor()
    cursor.execute(sql)
    total_consultados = cursor.rowcount 
    print(total_consultados)

    df = pd.read_sql_query(sql, conn)
    conn.close()    
    nome_arquivo_excel = 'arquivo/Conciliacao_Diaria_Antecipacao.xlsx'
    df.to_excel(nome_arquivo_excel, index=False)

    print(f'Arquivo Excel "{nome_arquivo_excel}" gerado com sucesso.')

   


def relatorio_diario():
    conn = conexao()
    sql  = """
        select  
    	    dr.id_venda,
	        dr.nu_parcela,
            to_char(dr.dt_pagamento, 'DD/MM/YYYY') as dt_pagamento,
            to_char(dt_prevista, 'DD/MM/YYYY') as dt_prevista ,
            dr.id_parcela ,		
            nm_ec ,
            dr.dt_recebimento,
            nm_tipo_pagamento ,
            nu_banco ,
            nm_banco ,
            nu_agencia ,
            nu_conta_bancaria ,
            dr.vl_parcela_bruto ,
            dr.vl_taxa,
            dr.tp_boleto_antecipado ,
            dr.vl_garantia ,
            dr.vl_excedente ,
            dr.vl_parc_antecipacao ,
            dr.vl_tx_antecipacao ,
            dr.vl_taxa_split ,
            dr.vl_taxa_gw ,
            dr.tx_base_adquirente ,
            dr.ds_taxa ,
            dr.tx_repasse ,	
            dr.vl_repasse,	
            nm_cliente_split,
            dr.id_tipo_antecipacao,
            dr.id_boleto_arbi,
            dr.ds_tid,
            dr.nm_adquirente 
        from dw_recebiveis dr
        inner join parcela p on dr.id_parcela = p.id_parcela
        inner join cliente c on dr.id_ec = c.id_cliente 
        where 
            dr.id_venda is not null	 
            and coalesce (dr.dt_recebimento, dt_prevista) between date_trunc('day', current_date) and cast(to_char(current_date, 'YYYY-MM-DD') ||' 23:59:59' as timestamp)
        order by dt_prevista asc, nm_ec asc, nm_cliente_split asc, dr.nm_tipo_pagamento asc
    """

    cursor = conn.cursor()

    cursor.execute(sql)
    total_consultados = cursor.rowcount 
    print(total_consultados)

    df = pd.read_sql_query(sql, conn)
    conn.close()    
    nome_arquivo_excel = 'arquivo/Conciliacao_Diaria.xlsx'
    df.to_excel(nome_arquivo_excel, index=False)

    print(f'Arquivo Excel "{nome_arquivo_excel}" gerado com sucesso.')


    