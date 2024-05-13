'''

@ Manoel Olegário

'''
from datetime import datetime, timedelta
import settings # dados confidenciais
import pandas as pd
from requests import post
from json import dumps
from time import sleep
from sqlalchemy import create_engine, inspect
import app # dados confidenciais
import numpy as np

# função para exportar os dados da API para o redshift
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
    if table not in inspector.get_table_names(schema='bi'):
        # A tabela não existe, você pode criar aqui se desejar.
        df.to_sql(table, engine, schema='bi', index=False, chunksize=10000, method='multi')
    else:
        # A tabela existe, então você pode adicionar dados a ela.
        df.to_sql(table, engine, schema='bi', index=False, if_exists='append', chunksize=10000, method='multi')
    print("-------------CONCLUÍDO-------------")

def ler_tabela_do_redshift(table, schema, data_corrida):
    print("Lendo tabela...")
    # Criar a URL de conexão com o banco de dados Redshift
    db_connection_url = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
        settings.database['redshift']['USER'],
        settings.database['redshift']['PASSWORD'],
        settings.database['redshift']['HOST'],
        settings.database['redshift']['PORT'],
        settings.database['redshift']['NAME'])
    # Criar uma conexão com o banco de dados
    engine = create_engine(db_connection_url)
    # Verificar se a tabela existe
    inspector = inspect(engine)
    if table in inspector.get_table_names(schema=schema):
        # Construir a consulta base
        query = f"SELECT * FROM {schema}.{table}"
        # Adicionar filtro de data, se fornecido
        if data_corrida:
            query += f" WHERE datacorrida = '{data_corrida}'"
        # Ler os dados da tabela usando o Pandas
        df = pd.read_sql_query(query, engine)
        print("Dados lidos com sucesso!")
        return df
    else:
        print("A tabela especificada não existe no esquema fornecido.")
        return None

# função que acessa a API da RJ para fazer a requisição
def busca_viagem(empresaId, servico, operator_name, trip_date):
    # os parâmetros passados são os que retornaram da planilha da função 'get_servs_in_bucket'
    #
    # criação da url de requisição
    url = '{}/api-gateway/integracao/padrao/bilhete/viagem'.format(settings.credentials[(operator_name)]['embai']['host'])
    body =  {'servico': str(servico),
        "data": str(trip_date),
        "empresa": str(empresaId)}
    headers = settings.credentials[operator_name]['embai']['headers']
    max_attempts = 1  # Número de tentativas ???

    for attempt in range(max_attempts):
        # realização da requisição passando a url o body contendo os dados da busca e o headers com as credenciais de autenticação
        r = post(url, data=dumps(body), headers=headers,
                          auth=settings.credentials[operator_name]['embai']['auth'], timeout=15)
        # caso o retorno for 200 presseguimos
        if r.status_code == 200:
            try:
                print('r.json() = ', len(r.json()))
                # salvamos a respota da requisição em um dataframe
                dfbv = pd.DataFrame.from_records(r.json())
                # o dataframe é retornado para após isso ser inserido no banco de daosdos
                return dfbv
            except:
                # em caso de erro um dataframe em branco é retornado para podermos prosseguir para a próxima etapa do loop
                dfbv = pd.DataFrame()
                return dfbv
        else:
        # caso o retorno for diferente de 200 aguardamos 4 minutos para a próxima requisição
            print("Exceeded API rate limit. Waiting and trying again.")
            sleep(240)
    print(f"Failed after {max_attempts} attempts")
    return pd.DataFrame()

# essa função foi criada para inserir a data de ontem no processo
# usamos uma função para isso, porque se precisar pegar um período maior basta inserir a data_inicio e a data_fim
def dias_no_periodo(data_inicio, data_fim):
    data_inicio = datetime.strptime(data_inicio, "%Y-%m-%d")
    data_fim = datetime.strptime(data_fim, "%Y-%m-%d") + timedelta(days=1)  # Adiciona um dia para incluir a data final
    data_atual = data_inicio
    periodo = []
    while data_atual < data_fim:
        periodo.append(data_atual.strftime('%Y-%m-%d'))
        data_atual += timedelta(days=1)
    return periodo # o retorno é uma list


def ope_operadora_rota(): #d-1
    data_inicio = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d') # diariamente  ambas as datas
    data_final = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d') # serão o dia de ontem
    df_operadora =  ler_tabela_do_redshift("disponibilidade_rota", "bi", data_corrida=None)
    df_operadora = df_operadora.drop_duplicates(subset=['servico']) # , 'rutaid', 'marcaid', 'grupo', 'grupoorigemid', 'grupodestinoid', 'datasaida', 'poltronastotal'
    print("DATAS:", str(data_inicio), str(data_final))
    operadora = "operadora"
    dias = dias_no_periodo(data_inicio, data_final) # passamos o dia de ontem para a função 'dias no período'
    #operadoras = ["operadora", "epc", "santo anjo", "brasil sul", "catedral"] # essas são as operadoras encontradas na RJ
    colunas_necessarias = ['transacaoId', 'transacaoIdOriginal', 'categoriaId', 'empresa', 
                                   'nomePassageiro', 'documentoPassageiro', 'documento2Passageiro', 
                                   'tipoDocumentoPassageiro', 'tipoDocumento2Passageiro', 'cpf', 
                                   'dataHoraVenda', 'dataHoraViagem', 'rutaId', 'numeroImpresso', 
                                   'numeroSistema', 'origem', 'destino', 'servico', 'status', 'tarifa', 
                                   'precoBase', 'pedagio', 'taxaEmbarque', 'outros', 'agenciaId', 
                                   'usuarioId', 'estacaoId', 'poltrona', 'tipoVenda', 'prefixo', 
                                   'motivoDesconto', 'numeroBpe', 'porcentagemDesconto', 'telefone'] # somente essas colunas serão usadas
    lista_dataframes = []
    for dia in dias: # iteramos sobre o dia pois se for passado um período de vários dias precisamos pegar de todos
        df_final_total = pd.DataFrame(columns=colunas_necessarias)
        for i, servico in df_operadora["servico"].items(): # iteramos sobre todos os servicos realizados naquele dia
            empresa_id = df_operadora["empresaid"] # salvamos os dados da coluna empresaId em uma lista
            print("Dia:", str(dia),"Operadora:", str("operadora"), "Empresa ID:", str(empresa_id[i]), "Servico ID:", str(servico))
            # Fazemos a requisição e salvamos o retorno na variável df_final, lembrando que o retorno é um dataframe
            try:
                df_final = busca_viagem(empresaId= empresa_id[i],servico = servico, operator_name= operadora,trip_date= dia)
                df_final = df_final.reindex(columns=colunas_necessarias, fill_value=None)
                df_final = df_final.astype(str)
                lista_dataframes.append(df_final)
                print("Encontrado!") # requisição ok
            except:
                print("Erro...")
                continue
        df_final_total = pd.concat(lista_dataframes, ignore_index=True) # concat da lista
        df_final_total.replace([np.inf, -np.inf], np.nan, inplace=True) # remove NaN e vazios
        df_final_total['agenciaId'] = df_final_total['agenciaId'].replace(['inf', '-inf', 'NaN', 'nan'], 0, regex=True) # remove NaN e vazios
        df_final_total['usuarioId'] = df_final_total['usuarioId'].replace(['inf', '-inf', 'NaN', 'nan'], 0, regex=True) # remove NaN e vazios

        df_final_total['agenciaId'] = df_final_total['agenciaId'].astype(float).astype(int)
        df_final_total['usuarioId'] = df_final_total['usuarioId'].astype(float).astype(int)
        quantidade_linhas = len(df_final_total) 
        df_final_total = df_final_total.drop_duplicates(subset=['empresa', 'nomePassageiro', 'documentoPassageiro', 'dataHoraVenda', 'origem', 'destino','servico', 'status', 'poltrona', 'numeroBpe'])
        linhas_unicas = len(df_final_total)
        linhas_removidas = quantidade_linhas - linhas_unicas
        print("Quantidade de registros: ", str(quantidade_linhas))
        print("Registros removidos: ", str(linhas_removidas))
        print("Registros a exportar: ", str(linhas_unicas))
    # por fim enviamos o dataframe retornado para o redshift
    exportar_tabela_para_redshift(df_final_total, "rota") # export
    return df_final_total[["origem", "destino"]], operadora # return para outra função
# função busca serviçso que vão operar no futuro
def main(): #d+1
    df_ret, operadora = ope_operadora_rota() # return tras as 
    df_ret.drop_duplicates(inplace=True)
    data_inicio = (datetime.now() - timedelta(days=-1)).strftime('%Y-%m-%d')
    data_final = (datetime.now() - timedelta(days=-1)).strftime('%Y-%m-%d')
    dias = dias_no_periodo(data_inicio, data_final)
    lista_dfs = []
    for indice, linha in df_ret.iterrows():  # iterando sobre as linhas do DataFrame
        origem = linha['origem']  # acessando o valor da coluna 'origem' para a linha atual
        destino = linha['destino']
        for dia in dias:
            df_dispo = app.busca_de_servicos(operadora, origem, destino, dia)
            colunas = ['servico', 'rutaId', 'prefixoLinha', 'marcaId', 'grupo', 'grupoOrigemId', 'grupoDestinoId', 'saida', 'chegada', 'dataCorrida', 'dataSaida', 'poltronasLivres', 'poltronasTotal', 'preco', 'precoOriginal', 'classe', 'empresa', 'empresaId', 'km', 'cnpj', 'modalidadeBpe']
            if not df_dispo.empty:
                df_dispo = df_dispo[colunas]
                lista_dfs.append(df_dispo)
            else:
                print("Dataframe vazio!")
                df_dispo = pd.DataFrame(columns=colunas)
                lista_dfs.append(df_dispo)
    df_final = pd.concat(lista_dfs)  # Concatenando todos os dataframes
    print(df_final.head())
    df_final.drop_duplicates(subset=['servico', 'rutaId', 'prefixoLinha', 'marcaId', 'grupo', 'grupoOrigemId', 'grupoDestinoId', 'saida', 'dataCorrida', 'poltronasTotal', 'empresaId', 'chegada'], inplace=True)
    exportar_tabela_para_redshift(df_final, "disponibilidade_rota")