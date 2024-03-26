# flake8: noqa
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os

def conciliar_cav_venda_credito():
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

    cursor = conn.cursor()

    sql = """
        SELECT 
            wctl.dt_pagamento
            , wctl.vl_liquido
            , wctl.nu_taxa
            , wctl.ds_tid
            , wctl.ds_arp
            , wctl.ds_nsu
            , wctl.nm_forma_pagamento
            , wctl.tp_lancamento
            , t.id_venda
            , t.id_parcela
        FROM webhook_cielo_table_log wctl
        LEFT JOIN transacao t ON wctl.ds_tid||LPAD(wctl.ds_arp::text, 6, '0')||LPAD(wctl.ds_nsu::text, 6, '0') = t.ds_tid||t.ds_arp||t.ds_nsu
        WHERE 
	        wctl.nm_forma_pagamento = 'Credito a vista' 
	        AND wctl.tp_lancamento = 'Venda credito' 
            and wctl.tp_conciliado = false   	
    """

    cursor.execute(sql)
    total_consultados = cursor.rowcount  # Total de registros consultados
    total_atualizados = 0  # Inicializa o contador de registros atualizados
    total_atualizados_cielo_log = 0

    # return total_consultados

    for row in cursor.fetchall():                 
        if update_cav_venda_credito(conn, row[0], row[1], row[2], row[6], row[7], row[3], row[4], row[5]):  
            total_atualizados += 1
            if update_wh_cielo_table_log(conn,  row[6], row[7], row[3], row[4], row[5]):
                total_atualizados_cielo_log += 1
        print(row[4], total_atualizados, total_atualizados_cielo_log)                

    # Fechar o cursor
    cursor.close()

    return total_consultados, total_atualizados, total_atualizados_cielo_log

def update_cav_venda_credito(conn, dt_pagamento, vl_liquido, nu_taxa, nm_forma_pagamento, tp_lancamento, ds_tid, ds_arp, ds_nsu):    
    cursor_u = conn.cursor()

    if ds_tid != '':
        join = " wctl.ds_tid||LPAD(wctl.ds_arp::text, 6, '0')||LPAD(wctl.ds_nsu::text, 6, '0') = t.ds_tid||t.ds_arp||t.ds_nsu "
        filtro = " and wctl.ds_canal_venda not in ('TEF') "
    else:
        join = " wLPAD(wctl.ds_arp::text, 6, '0')||LPAD(wctl.ds_nsu::text, 6, '0') = t.ds_arp||t.ds_nsu "
        filtro = " and wctl.ds_canal_venda in ('TEF' ) "

# --ex_dt_prevista_recebimento = CAST(%s AS TIMESTAMP)

    update_sql = """
        UPDATE parcela SET 
            dt_credito = CAST(%s AS TIMESTAMP),
            vl_credito = CAST(%s AS NUMERIC(12,2)),
            vl_taxa_credito = CAST(%s AS NUMERIC(12,2))             
        FROM webhook_cielo_table_log wctl
        LEFT JOIN transacao t ON """ + join + """  
        WHERE 
            parcela.id_parcela = t.id_parcela
            AND parcela.dt_credito IS NULL
            and wctl.nm_forma_pagamento = %s
            and wctl.tp_lancamento =  %s 
            and wctl.ds_tid =  %s 
            and wctl.ds_arp =  %s 
            and wctl.ds_nsu =  %s 
            """ + filtro
    
    

    # Executar a consulta de atualização com os parâmetros fornecidos
    cursor_u.execute(update_sql, (dt_pagamento, vl_liquido, nu_taxa, nm_forma_pagamento, tp_lancamento, ds_tid, ds_arp, ds_nsu))  
    conn.commit()
    cursor_u.close()    

    return cursor_u.rowcount > 0


def update_wh_cielo_table_log(conn, nm_forma_pagamento, tp_lancamento, ds_tid, ds_arp, ds_nsu):
    
    cursor_u = conn.cursor()

    update_sql = """
        update webhook_cielo_table_log set 
            tp_conciliado = true
        where          
            nm_forma_pagamento = %s
            and tp_lancamento =  %s    
            and ds_tid = %s
            and ds_arp = %s
            and ds_nsu = %s
            and tp_conciliado = false

        """
          
    # Executar a consulta de atualização com os parâmetros fornecidos
    cursor_u.execute(update_sql, (nm_forma_pagamento, tp_lancamento, ds_tid, ds_arp, ds_nsu))  
    conn.commit()
    cursor_u.close()    

    return cursor_u.rowcount > 0