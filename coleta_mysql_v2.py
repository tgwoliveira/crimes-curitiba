#######################################################################

# Script de Coleta e Carga de Dados - Crimes Curitiba

#######################################################################



# Versão: Local MySQL

# Autor: Thiago Oliveira

# Data: 2024-06-20



# Este script:

# 1. Busca os links dos CSVs de ocorrências criminais (2016-2025)

# 2. Baixa e processa os dados diretamente da internet

# 3. Carrega no banco MySQL local seguindo modelo dimensional



#######################################################################

from datetime import datetime

import pandas as pd

import requests

from bs4 import BeautifulSoup

from urllib.parse import urljoin

import sys



# Bibliotecas para o ETL Relacional

from sqlalchemy import create_engine, text

from sqlalchemy.exc import IntegrityError



# ============================================================

# CONFIGURAÇÕES GLOBAIS

# ============================================================



# URLs dos portais de dados

URL_PORTAL_ANTIGO = "https://dadosabertos.c3sl.ufpr.br/curitiba/Sigesguarda/"

URL_PORTAL_NOVO = "https://dadosabertos.curitiba.pr.gov.br/conjuntodado/detalhe?chave=b16ead9d-835e-41e8-a4d7-dcc4f2b4b627"



# Cabeçalhos HTTP

HEADERS = {

    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',

    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',

}



# Colunas esperadas nos CSVs

CSV_COLUMNS = [

    'OCORRENCIA_DATA', 'OCORRENCIA_ANO', 'OCORRENCIA_MES', 'OCORRENCIA_HORA',

    'OCORRENCIA_DIA_SEMANA', 'OCORRENCIA_PERIODO', 'ATENDIMENTO_BAIRRO_NOME',

    'ATENDIMENTO_REGIONAL_NOME', 'ATENDIMENTO_LOGRADOURO_NOME',

    'CLASSIFICACAO_BAIRRO_REGIONAL', 'NATUREZA1_CODIGO', 'NATUREZA1_DESCRICAO',

    'NATUREZA2_DESCRICAO', 'TIPO_ENVOLVIMENTO', 'ATENDIMENTO_NUMERO'

]



# Configuração do banco MySQL LOCAL

DB_CONFIG = {

    'host': '127.0.0.1',

    'port': 3306,

    'user': 'root',  # ALTERE conforme seu usuário MySQL

    'password': 'thiagohc',  # ALTERE para sua senha MySQL

    'database': 'crimes_curitiba'

}



# ============================================================

# FUNÇÕES DE COLETA (Web Scraping)

# ============================================================



# BUSCA LINKS DOS CSVs ANTIGOS (2016-2024)

def get_csv_links_antigos():

    # Fazemos um set chamado links e adicionamos os links encontrados

    links = set()

   

    try:

        response = requests.get(URL_PORTAL_ANTIGO, headers=HEADERS)

        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

       

        # percorremos todas as tags <a> que possuem o atributo href

        for link in soup.find_all('a', href=True):

            # Pegamos o valor do atributo href

            href = link.get('href')

            # Se o primeiro caractere for um dígito e o nome do arquivo conter '_sigesguarda_-_base_de_dados.csv', consideramos como link válido

            if href[0].isdigit() and '_sigesguarda_-_base_de_dados.csv' in href.lower():

                # Construímos a URL completa usando urljoin para garantir que seja um link absoluto

                full_url = urljoin(URL_PORTAL_ANTIGO, href)

                # Adicionamos a URL completa ao set de links

                links.add(full_url)

       

        # Ordenamos os links para garantir que sejam processados do mais antigo para o mais recente

        old_links = sorted(links)

        return old_links



    except requests.exceptions.RequestException as e:

        print(f"❌ Erro ao acessar o portal {URL_PORTAL_ANTIGO}: {e}")

        return []



# a prefeitura mudou o portal e não foi encontrado os dados de 2025 até o dia de hoje (2024-06-20)

# teremos que fazer um scraping desses dados uma outra hora

# BUSCA LINKS DOS CSVs DE 2025

def get_csv_links_2025():

    links = []

   

    try:

        response = requests.get(URL_PORTAL_NOVO, headers=HEADERS, timeout=30)

        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

       

        for a in soup.find_all('a', href=True):

            href = a['href']

            if href.endswith('.csv') and '2025' in href and 'Sigesguarda' in href:

                if href.startswith('http'):

                    links.append(href)

                else:

                    links.append(urljoin(URL_PORTAL_NOVO, href))

       

        print(f"   ✅ Encontrados {len(links)} CSVs de 2025")

        return links

       

    except Exception as e:

        print(f"   ❌ Erro ao buscar CSVs de 2025: {e}")

        return []





# ============================================================

# FUNÇÕES AUXILIARES PARA DIMENSÕES

# ============================================================



def classificar_periodo_dia(hora_str):

    """Classifica o período do dia baseado na hora"""

    try:

        hora = int(hora_str.split(':')[0]) if ':' in str(hora_str) else int(hora_str)

        if 0 <= hora < 6:

            return 'MADRUGADA'

        elif 6 <= hora < 12:

            return 'MANHÃ'

        elif 12 <= hora < 18:

            return 'TARDE'

        else:

            return 'NOITE'

    except:

        return 'NÃO INFORMADO'





def extrair_categoria_crime(descricao):

    """Extrai categoria principal do crime"""

    if not descricao or descricao == 'NAN':

        return 'NÃO INFORMADO'

   

    descricao_upper = str(descricao).upper()

   

    if 'FURTO' in descricao_upper:

        return 'FURTO'

    elif 'ROUBO' in descricao_upper:

        return 'ROUBO'

    elif 'LESÃO' in descricao_upper or 'LESAO' in descricao_upper:

        return 'LESÃO CORPORAL'

    elif 'DROGA' in descricao_upper or 'ENTORPECENTE' in descricao_upper:

        return 'DROGAS'

    elif 'TRÂNSITO' in descricao_upper or 'TRANSITO' in descricao_upper:

        return 'TRÂNSITO'

    elif 'AMEAÇA' in descricao_upper:

        return 'AMEAÇA'

    elif 'DANO' in descricao_upper:

        return 'DANO AO PATRIMÔNIO'

    elif 'HOMICÍDIO' in descricao_upper or 'HOMICIDIO' in descricao_upper:

        return 'HOMICÍDIO'

    else:

        return 'OUTROS'





def get_nome_mes(mes_num):

    """Retorna nome do mês"""

    meses = ['', 'JANEIRO', 'FEVEREIRO', 'MARÇO', 'ABRIL', 'MAIO', 'JUNHO',

             'JULHO', 'AGOSTO', 'SETEMBRO', 'OUTUBRO', 'NOVEMBRO', 'DEZEMBRO']

    try:

        return meses[int(mes_num)]

    except:

        return 'NÃO INFORMADO'





# ============================================================

# FUNÇÕES DE INSERÇÃO NO BANCO

# ============================================================



def get_or_create_tempo(connection, data_completa, ano, mes, dia_semana, periodo):

    """Busca ou cria registro na DIM_TEMPO"""

    try:

        # Buscar ID existente

        sql_select = text("""

            SELECT tempo_id FROM DIM_TEMPO

            WHERE data_completa = :data_completa

        """)

        result = connection.execute(sql_select, {'data_completa': data_completa}).fetchone()

       

        if result:

            return result[0]

       

        # Inserir novo

        data_obj = datetime.strptime(data_completa, '%Y-%m-%d')

       

        sql_insert = text("""

            INSERT INTO DIM_TEMPO

            (data_completa, ocorrencia_ano, ocorrencia_mes, ocorrencia_dia,

             ocorrencia_dia_semana, ocorrencia_periodo, nome_mes, trimestre, semestre)

            VALUES

            (:data_completa, :ano, :mes, :dia, :dia_semana, :periodo,

             :nome_mes, :trimestre, :semestre)

        """)

       

        trimestre = (int(mes) - 1) // 3 + 1

        semestre = 1 if int(mes) <= 6 else 2

       

        result = connection.execute(sql_insert, {

            'data_completa': data_completa,

            'ano': ano,

            'mes': mes,

            'dia': data_obj.day,

            'dia_semana': dia_semana,

            'periodo': periodo,

            'nome_mes': get_nome_mes(mes),

            'trimestre': trimestre,

            'semestre': semestre

        })

       

        return result.lastrowid

       

    except IntegrityError:

        # Caso de concorrência, busca novamente

        result = connection.execute(sql_select, {'data_completa': data_completa}).fetchone()

        return result[0] if result else None





def get_or_create_natureza(connection, nat1_codigo, nat1_desc, nat2_desc, tipo_envolvimento):

    """Busca ou cria registro na DIM_NATUREZA"""

    try:

        sql_select = text("""

            SELECT natureza_id FROM DIM_NATUREZA

            WHERE COALESCE(natureza1_descricao, '') = COALESCE(:nat1_desc, '')

            AND COALESCE(natureza2_descricao, '') = COALESCE(:nat2_desc, '')

            AND COALESCE(tipo_envolvimento, '') = COALESCE(:tipo_env, '')

        """)

       

        result = connection.execute(sql_select, {

            'nat1_desc': nat1_desc,

            'nat2_desc': nat2_desc,

            'tipo_env': tipo_envolvimento

        }).fetchone()

       

        if result:

            return result[0]

       

        # Inserir novo

        categoria = extrair_categoria_crime(nat1_desc)

       

        sql_insert = text("""

            INSERT INTO DIM_NATUREZA

            (natureza1_codigo, natureza1_descricao, natureza2_descricao,

             tipo_envolvimento, categoria_crime)

            VALUES (:codigo, :nat1, :nat2, :tipo_env, :categoria)

        """)

       

        result = connection.execute(sql_insert, {

            'codigo': nat1_codigo,

            'nat1': nat1_desc,

            'nat2': nat2_desc,

            'tipo_env': tipo_envolvimento,

            'categoria': categoria

        })

       

        return result.lastrowid

       

    except IntegrityError:

        result = connection.execute(sql_select, {

            'nat1_desc': nat1_desc,

            'nat2_desc': nat2_desc,

            'tipo_env': tipo_envolvimento

        }).fetchone()

        return result[0] if result else None

    except Exception as e:

        print(f"   DEBUG get_or_create_natureza: {e}")

        return None





def get_or_create_local(connection, bairro, regional, logradouro, classificacao):

    """Busca ou cria registro na DIM_LOCAL"""

    try:

        sql_select = text("""

            SELECT local_id FROM DIM_LOCAL

            WHERE COALESCE(bairro_nome, '') = COALESCE(:bairro, '')

            AND COALESCE(regional_nome, '') = COALESCE(:regional, '')

            AND COALESCE(logradouro_nome, '') = COALESCE(:logradouro, '')

        """)

       

        result = connection.execute(sql_select, {

            'bairro': bairro,

            'regional': regional,

            'logradouro': logradouro

        }).fetchone()

       

        if result:

            return result[0]

       

        # Inserir novo

        sql_insert = text("""

            INSERT INTO DIM_LOCAL

            (bairro_nome, regional_nome, logradouro_nome, classificacao_bairro_regional)

            VALUES (:bairro, :regional, :logradouro, :classificacao)

        """)

       

        result = connection.execute(sql_insert, {

            'bairro': bairro,

            'regional': regional,

            'logradouro': logradouro,

            'classificacao': classificacao

        })

       

        return result.lastrowid

       

    except IntegrityError:

        result = connection.execute(sql_select, {

            'bairro': bairro,

            'regional': regional,

            'logradouro': logradouro

        }).fetchone()

        return result[0] if result else None

    except Exception as e:

        print(f"   DEBUG get_or_create_local: {e}")

        return None





def get_or_create_hora(connection, hora_str):

    """Busca ou cria registro na DIM_HORA"""

    try:

        if not hora_str or pd.isna(hora_str):

            return None

           

        sql_select = text("""

            SELECT hora_id FROM DIM_HORA

            WHERE hora_completa = :hora_completa

        """)

       

        result = connection.execute(sql_select, {'hora_completa': hora_str}).fetchone()

       

        if result:

            return result[0]

       

        # Inserir novo

        try:

            hora_parts = str(hora_str).split(':')

            hora_num = int(float(hora_parts[0])) if len(hora_parts) > 0 else 0

            minuto_num = int(float(hora_parts[1])) if len(hora_parts) > 1 else 0

        except:

            hora_num = 0

            minuto_num = 0

           

        periodo = classificar_periodo_dia(hora_str)

       

        sql_insert = text("""

            INSERT INTO DIM_HORA

            (hora_completa, hora, minuto, periodo_dia)

            VALUES (:hora_completa, :hora, :minuto, :periodo)

        """)

       

        result = connection.execute(sql_insert, {

            'hora_completa': hora_str,

            'hora': hora_num,

            'minuto': minuto_num,

            'periodo': periodo

        })

       

        return result.lastrowid

       

    except IntegrityError:

        result = connection.execute(sql_select, {'hora_completa': hora_str}).fetchone()

        return result[0] if result else None

    except Exception as e:

        print(f"   DEBUG get_or_create_hora: {e}")

        return None





# ============================================================

# FUNÇÃO PRINCIPAL DE PROCESSAMENTO

# ============================================================



def processar_csv_para_mysql(csv_url, engine):

    """

    Lê CSV, visualiza dados, converte tipos e carrega no MySQL

    SEM fazer tratamento dos dados - apenas conversão de tipos

    """

    print(f"\n📥 Processando: {csv_url.split('/')[-1]}")

   

    try:

        # Ler o CSV

        try:

            df = pd.read_csv(

                csv_url,

                sep=";",

                encoding="utf-8",

                low_memory=False,

                usecols=lambda col: col in CSV_COLUMNS

            )

        except UnicodeDecodeError:

            df = pd.read_csv(

                csv_url,

                sep=";",

                encoding="latin1",

                low_memory=False,

                usecols=lambda col: col in CSV_COLUMNS

            )

        except ValueError:

            print(f"   ⚠️  Colunas não encontradas. Pulando arquivo.")

            return

       

        print(f"   📊 {len(df)} registros encontrados")

        print(f"   📋 Colunas: {list(df.columns)}")

        print(f"\n   Primeiros registros:")

        print(df.head(3).to_string())

        print()

       

        # Conversão de tipos (sem limpeza/tratamento)

        # Apenas converter para os tipos esperados pelo banco

       

        # Converter OCORRENCIA_DATA para formato correto

        if 'OCORRENCIA_DATA' in df.columns:

            df['OCORRENCIA_DATA'] = pd.to_datetime(

                df['OCORRENCIA_DATA'],

                format='%d/%m/%Y',

                errors='coerce'

            ).dt.strftime('%Y-%m-%d')

       

        # Converter colunas numéricas

        numeric_cols = ['NATUREZA1_CODIGO', 'ATENDIMENTO_NUMERO']

        for col in numeric_cols:

            if col in df.columns:

                df[col] = pd.to_numeric(df[col], errors='coerce')

       

        # Processar linha por linha - inserir no banco

        registros_inseridos = 0

        registros_erro = 0

       

        with engine.connect() as connection:

            transaction = connection.begin()

           

            try:

                for index, row in df.iterrows():

                    try:

                        # Validar se data existe

                        data_completa = row.get('OCORRENCIA_DATA')

                        if not data_completa or pd.isna(data_completa):

                            registros_erro += 1

                            continue

                       

                        # Buscar/criar dimensões

                        tempo_id = get_or_create_tempo(

                            connection,

                            data_completa,

                            row.get('OCORRENCIA_ANO'),

                            row.get('OCORRENCIA_MES'),

                            row.get('OCORRENCIA_DIA_SEMANA'),

                            row.get('OCORRENCIA_PERIODO')

                        )

                       

                        natureza_id = get_or_create_natureza(

                            connection,

                            row.get('NATUREZA1_CODIGO'),

                            row.get('NATUREZA1_DESCRICAO'),

                            row.get('NATUREZA2_DESCRICAO'),

                            row.get('TIPO_ENVOLVIMENTO')

                        )

                       

                        local_id = get_or_create_local(

                            connection,

                            row.get('ATENDIMENTO_BAIRRO_NOME'),

                            row.get('ATENDIMENTO_REGIONAL_NOME'),

                            row.get('ATENDIMENTO_LOGRADOURO_NOME'),

                            row.get('CLASSIFICACAO_BAIRRO_REGIONAL')

                        )

                       

                        hora_id = get_or_create_hora(

                            connection,

                            row.get('OCORRENCIA_HORA')

                        )

                       

                        # Inserir na tabela fato

                        sql_fato = text("""

                            INSERT INTO FATO_OCORRENCIA

                            (tempo_id, natureza_id, local_id, hora_id, atendimento_numero)

                            VALUES (:tempo_id, :natureza_id, :local_id, :hora_id, :atendimento)

                        """)

                       

                        connection.execute(sql_fato, {

                            'tempo_id': tempo_id,

                            'natureza_id': natureza_id,

                            'local_id': local_id,

                            'hora_id': hora_id,

                            'atendimento': row.get('ATENDIMENTO_NUMERO')

                        })

                       

                        registros_inseridos += 1

                       

                        if registros_inseridos % 1000 == 0:

                            print(f"   ⏳ {registros_inseridos} registros processados...")

                       

                    except Exception as e_row:

                        registros_erro += 1

                        if registros_erro < 5:

                            print(f"   ⚠️  Erro na linha {index}: {e_row}")

               

                transaction.commit()

                print(f"   ✅ {registros_inseridos} registros inseridos com sucesso!")

                if registros_erro > 0:

                    print(f"   ⚠️  {registros_erro} registros com erro (pulados)")

               

            except Exception as e:

                transaction.rollback()

                print(f"   ❌ Erro fatal. Rollback realizado: {e}")

               

    except Exception as e:

        print(f"   ❌ Erro ao processar CSV: {e}")



# ============================================================

# FUNÇÃO PRINCIPAL

# ============================================================



def main():

    print("# SISTEMA DE COLETA E CARGA - CRIMES CURITIBA")



    ###############################################################

    # 1. Criar engine de conexão MySQL

    try:

        connection_string = (

            f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"

            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

            f"?charset=utf8mb4"

        )

       

        engine = create_engine(connection_string, pool_pre_ping=True)

       

        # Testar conexão

        with engine.connect() as conn:

            conn.execute(text("SELECT 1"))

       

        print("Conexão com o Banco de Dados MySQL estabelecida!")

       

    except Exception as e:

        print(f"\nERRO DE CONEXÃO COM MYSQL:")

        print(f"   {e}\n")

        print("\nVerifique:")

        print("   1. MySQL está rodando?")

        print("   2. Usuário e senha estão corretos?")

        print("   3. Banco 'crimes_curitiba' foi criado? (Execute setup_database.sql)")

        sys.exit(1)

   

    ##############################################################

    # 2. Buscar links dos CSVs

    # pegamos os links dos CSVs antigos (2016-2024)

    links_antigos = get_csv_links_antigos()

    # os dados de 2025 até o momento não foram encontrados = modificar esta função uma outra hora

    #links_2025 = get_csv_links_2025()

    links_2025 = []



    # Combine todos os links em uma única lista

    todos_links = links_antigos + links_2025

   

    # Se nenhum link encontrado, encerrar

    if not todos_links:

        print("\n  Nenhum CSV encontrado. Verifique sua conexão com a internet.")

        return

   

    # Todos os links encontrados

    for i in todos_links:

        print(i)



    ##############################################################

    # 3. Processar cada CSV

    # Percorremos cada link e processamos

    for i, url in enumerate(todos_links, 1):

        print(f"\n[{i}/{len(todos_links)}]", end=" ")

        processar_csv_para_mysql(url, engine)

   

    '''

    # 4. Estatísticas finais

    print("\n" + "=" * 70)

    print("📊 ESTATÍSTICAS FINAIS")

    print("=" * 70)

   

    with engine.connect() as conn:

        stats = conn.execute(text("""

            SELECT

                (SELECT COUNT(*) FROM FATO_OCORRENCIA) AS total_ocorrencias,

                (SELECT COUNT(DISTINCT tempo_id) FROM FATO_OCORRENCIA) AS datas_distintas,

                (SELECT COUNT(*) FROM DIM_NATUREZA) AS tipos_crime,

                (SELECT COUNT(*) FROM DIM_LOCAL) AS locais,

                (SELECT MIN(data_completa) FROM DIM_TEMPO) AS data_inicial,

                (SELECT MAX(data_completa) FROM DIM_TEMPO) AS data_final

        """)).fetchone()

       

        print(f"\n✅ Total de Ocorrências: {stats[0]:,}")

        print(f"📅 Período: {stats[4]} a {stats[5]}")

        print(f"📍 Locais cadastrados: {stats[3]}")

        print(f"🔍 Tipos de crime: {stats[2]}")

        print(f"⏱️  Tempo total: {tempo_total:.2f} minutos")

       

    print("\n" + "=" * 70)

    print("✅ PROCESSO CONCLUÍDO COM SUCESSO!")

    print("=" * 70)

    print("\n💡 Próximos passos:")

    print("   1. Abra o Jupyter Notebook: notebooks/analise_dados.ipynb")

    print("   2. Ou conecte o Power BI ao banco MySQL local")

    print("   3. Ou execute consultas SQL diretamente no MySQL Workbench")

    print("\n")

    '''



if __name__ == "__main__":

    main()