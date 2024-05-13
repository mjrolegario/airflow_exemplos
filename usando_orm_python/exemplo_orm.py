"""
@ manoel olegário
10.04.2024
"""

import pandas as pd
from sqlalchemy import create_engine, inspect
import settings
from time import sleep

dtype={'valor_do_cbk': float}

def ler_tabela_do_postgres():
    print("Lendo tabela...")
    # Criar a URL de conexão com o banco de dados Redshift
    db_connection_url = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
                        settings.database['default']['USER'],
                        settings.database['default']['PASSWORD'],
                        settings.database['default']['HOST'],
                        settings.database['default']['PORT'],
                        settings.database['default']['NAME'])
    # Criar uma conexão com o banco de dados
    engine = create_engine(db_connection_url)
    # Verificar se a tabela existe
    if True:
        # Construir a consulta base
        query = """select
    distinct(tabela_main.ord_id) as id_da_venda,
    tabela_main.ord_public_id as id_da_konduto,
    tabela_main.pay_created_at as data_de_pagamento,
    cast(tabela2.amount_disputed AS FLOAT) as valor_do_cbk,
    tabela_main.cus_created_at as data_do_cadastro,
    tabela_main.cus_id as id_do_cliente,
    tabela_main.cus_name as nome_do_cliente,
    tabela_main.cus_cpf as documento_do_cliente,
    tabela_main.cus_email as email_do_cliente,
    tabela_main.cus_birthdate as data_de_nascimento_do_cliente,
    (tabela_main.cus_birthdate - tabela_main.pay_created_at) as idade_titular,
    tabela_main.seat_name as nome_do_passageiro,
    tabela_main.sales_platform_group as plataforma_de_venda,
    tabela_main.cms_name as cms_nome_do_grupo,
    tabela_main.ope_name as operadora,
    tabela_main.channel as canal,
    tabela_main.ori_state as estado_de_origem,
    tabela_main.ori_name as origem,
    tabela_main.des_state as estado_de_destino,
    tabela_main.des_name as destino,
    tabela_main.ori_name || ' - ' ||tabela_main.des_name as rota,
    tabela_main.res_departure_at as data_da_viagem,
    tabela_main.gateway_id as id_de_pagamento,
    tabela_main.gateway_payment as meio_de_pagamento,
    tabela_main.pay_payment_method as metodo_de_pagamento,
    tabela_main.pay_card_type as bandeira,
    tabela_main.pay_issuing_bank as banco,
    tabela_main.pay_bin as seis_digitos_cartao,
    tabela_main.pay_last_4 as quatro_digitos_cartao,
    tabela_main.pay_card_holder_name as titular_do_cartao,
    tabela_main.pay_holder_birthdate as data_de_nasc_do_titular_do_cartao,
    tabela_main.pay_holder_phone as telefone_do_titular_do_cartao,
    tabela_main.pay_installments as qtd_de_parcelas,
    (case when tabela_main.ori_state like tabela_main.des_state then 'estadual'
    else 'interestadual'
    end) as tipo_de_viagem,
    (tabela_main.res_departure_at - tabela_main.pay_created_at) as antecipacao,
    tabela2.dispute_created_at as data_da_disputa,
    tabela2.kind as tipo_de_disputa,
    tabela2.status as status_da_disputa,
    tabela2.reason as motivo_da_disputa,
    tabela2.received_date as data_de_recebimento_da_disputa,
    tabela2.reply_by_date as disputar_ate,
    (tabela2.reply_by_date - tabela2.received_date)as prazo_de_disputa,
    (tabela2.received_date - tabela_main.pay_created_at) as maturacao_de_cbk,
    (case when tabela_main.res_departure_at > tabela2.received_date then 'nao finalizada'
    else 'finalizada'
    end) as status_da_viagem,
    tabela2.updated_at as ultima_atualizacao
from
    tickets_all as tabela_main
inner join paypal_hooks as tabela2 on tabela_main.ord_id = tabela2.order_id
"""

        # Ler os dados da tabela usando o Pandas
        df = pd.read_sql_query(query, engine, dtype=dtype)
        print("Dados lidos com sucesso!")
        return df

def exportar_tabela_para_redshift(df, table):
    print("Exportando tabela...")
    db_connection_url = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
        settings.database['redshift']['USER'],
        settings.database['redshift']['PASSWORD'],
        settings.database['redshift']['HOST'],
        settings.database['redshift']['PORT'],
        settings.database['redshift']['NAME'])
    engine = create_engine(db_connection_url)
    inspector = inspect(engine)
    if table in inspector.get_table_names(schema='bi'):
        # Exclui a tabela
        engine.execute(f"DROP TABLE IF EXISTS bi.{table}")
    
    df.to_sql(table, engine, schema='bi', index=False, chunksize=10000, method='multi')
    print("-------------CONCLUÍDO-------------")

def publicar_tabela(table):
    print("Buscando tabela...")
    db_connection_url = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
        settings.database['redshift']['USER'],
        settings.database['redshift']['PASSWORD'],
        settings.database['redshift']['HOST'],
        settings.database['redshift']['PORT'],
        settings.database['redshift']['NAME'])
    engine = create_engine(db_connection_url)
    inspector = inspect(engine)
    if table in inspector.get_table_names(schema='bi'):
        # Exclui a tabela
        engine.execute(f"GRANT SELECT ON bi.{table} TO PUBLIC;")
    
    print("-------------TABELA PUBLICADA-------------")

def main_orm():
    df = ler_tabela_do_postgres()
    exportar_tabela_para_redshift(df, ("base_main"))
    sleep(5)
    publicar_tabela("base_main")
