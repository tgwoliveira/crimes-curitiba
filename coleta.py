import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin
import time
import sys

# Bibliotecas para o ETL Relacional
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError, IntegrityError

# --- CONFIGURAÇÕES GLOBAIS ---

# URL da página do Portal de Dados que contém a tabela com os links (2016-2024)
# Mantenha esta URL que funcionava para o scraping da tabela.
URL_PORTAL = "https://dadosabertos.c3sl.ufpr.br/curitiba/Sigesguarda/"

# Cabeçalhos para simular um navegador e evitar bloqueios
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': URL_PORTAL,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
}
CHUNK_SIZE = 1024 * 1024 # 1 MB

# Colunas que serão extraídas dos CSVs e utilizadas no ETL.
CSV_COLUMNS = [
    'OCORRENCIA_DATA', 'OCORRENCIA_ANO', 'OCORRENCIA_MES', 'OCORRENCIA_HORA', 
    'OCORRENCIA_DIA_SEMANA', 'OCORRENCIA_PERIODO', 'ATENDIMENTO_BAIRRO_NOME', 
    'ATENDIMENTO_REGIONAL_NOME', 'ATENDIMENTO_LOGRADOURO_NOME', 
    'CLASSIFICACAO_BAIRRO_REGIONAL', 'NATUREZA1_CODIGO', 'NATUREZA1_DESCRICAO', 
    'NATUREZA2_DESCRICAO', 'TIPO_ENVOLVIMENTO', 'ATENDIMENTO_NUMERO' 
]

# --- FUNÇÕES DE COLETA (Scraping e Download) ---

def get_old_csv_links(portal_url):
    """Busca links para a base de dados OCORRÊNCIAS CRIMINAIS (2016-2024) no portal UFPR."""
    print(f"🌐 Buscando links na página do portal: {portal_url}")
    links = set()
    try:
        response = requests.get(portal_url, headers=HEADERS)
        response.raise_for_status() 
        soup = BeautifulSoup(response.text, 'html.parser')

        for link in soup.find_all('a', href=True):
            href = link.get('href')

            # Filtra links que parecem ser os arquivos CSV anuais
            if href and '.csv' in href.lower() and ('ocorrencias-criminais' in href.lower() or 'base_de_dados' in href.lower()):
                full_url = urljoin(portal_url, href)
                links.add(full_url)
        
        # Filtro para garantir que não pega a URL problemática do novo portal (mid-dadosabertos)
        old_links = [l for l in links if 'mid-dadosabertos' not in l]

        print(f"✅ Encontrados {len(old_links)} links de 2016-2024 (via portal).")
        return old_links

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao acessar o portal {portal_url}: {e}")
        return []


# --- NOVA FUNÇÃO: Coleta de links CSV 2025 do novo portal ---
def get_2025_csv_links():
    """
    Busca links dos arquivos CSV de 2025 no novo portal de dados abertos de Curitiba.
    """
    print("\n🌐 Buscando links de 2025 no novo portal...")
    # URL da página de detalhe do conjunto de dados
    page_url = "https://dadosabertos.curitiba.pr.gov.br/conjuntodado/detalhe?chave=b16ead9d-835e-41e8-a4d7-dcc4f2b4b627"
    try:
        response = requests.get(page_url, headers=HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.endswith('.csv') and '2025' in href and 'Sigesguarda' in href:
                # Corrige link relativo se necessário
                if href.startswith('http'):
                    links.append(href)
                else:
                    links.append(urljoin(page_url, href))
        print(f"✅ Encontrados {len(links)} links de 2025.")
        return links
    except Exception as e:
        print(f"❌ Erro ao coletar links de 2025: {e}")
        return []

def download_file(url, target_path):
    """Baixa um arquivo com tratamento de erro."""
    print("Função de download desativada. Não salva arquivos em disco.")
    return False

# ---------------------------------------------------------------------
# --- FUNÇÃO PARA PROCESSAR CSVs DIRETO DA INTERNET ---

def process_csv_url_to_db(csv_url, engine):
    """
    Baixa o CSV diretamente da internet, processa em memória e carrega no banco de dados.
    """
    print(f"\n🌐 Processando CSV direto da internet: {csv_url}")
    try:
        try:
            df = pd.read_csv(csv_url, sep=";", encoding="utf-8", low_memory=False, usecols=lambda col: col in CSV_COLUMNS)
        except UnicodeDecodeError:
            df = pd.read_csv(csv_url, sep=";", encoding="latin1", low_memory=False, usecols=lambda col: col in CSV_COLUMNS)
        except ValueError:
            print("   ❌ Erro de Coluna: O CSV não tem as colunas esperadas. Pulando.")
            return

        # Limpeza e normalização de strings
        df = df.apply(lambda x: x.astype(str).str.strip().str.upper().replace('NAN', None).replace('NONE', None).replace('', None) if x.dtype == 'object' else x)
        df['OCORRENCIA_DATA'] = pd.to_datetime(df['OCORRENCIA_DATA'], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d')

        # Carregamento para o banco de dados
        with engine.connect() as connection:
            for index, row in df.iterrows():
                # --- DIMENSÃO TEMPO (TEMPO) ---
                if row['OCORRENCIA_DATA']:
                    tempo_data = {
                        'data_completa': row['OCORRENCIA_DATA'],
                        'ocorrencia_ano': row['OCORRENCIA_ANO'],
                        'ocorrencia_mes': row['OCORRENCIA_MES'],
                        'ocorrencia_dia_semana': row['OCORRENCIA_DIA_SEMANA'],
                        'ocorrencia_periodo': row['OCORRENCIA_PERIODO'],
                    }
                    tempo_id = get_or_create_dimension(connection, 'TEMPO', ['data_completa'], tempo_data)
                else:
                    continue

                # --- DIMENSÃO NATUREZA (NATUREZA) ---
                natureza_data = {
                    'natureza1_descricao': row.get('NATUREZA1_DESCRICAO') or 'NAO INFORMADO',
                    'natureza2_descricao': row.get('NATUREZA2_DESCRICAO'),
                    'tipo_envolvimento': row.get('TIPO_ENVOLVIMENTO'),
                }
                natureza_id = get_or_create_dimension(connection, 'NATUREZA', ['natureza1_descricao', 'natureza2_descricao', 'tipo_envolvimento'], natureza_data)

                # --- DIMENSÃO LOCAL (LOCAL) ---
                local_data = {
                    'bairro_nome': row.get('ATENDIMENTO_BAIRRO_NOME') or 'NAO INFORMADO',
                    'regional_nome': row.get('ATENDIMENTO_REGIONAL_NOME'),
                    'logradouro_nome': row.get('ATENDIMENTO_LOGRADOURO_NOME'),
                    'classificacao_bairro_regional': row.get('CLASSIFICACAO_BAIRRO_REGIONAL'),
                }
                local_id = get_or_create_dimension(connection, 'LOCAL', ['bairro_nome', 'regional_nome', 'logradouro_nome'], local_data)

                # --- TABELA FATO (OCORRENCIA) ---
                fato_sql = text("""
                    INSERT INTO public.OCORRENCIA (tempo_id, natureza_id, local_id, ocorrencia_hora)
                    VALUES (:tempo_id, :natureza_id, :local_id, :ocorrencia_hora)
                """)
                connection.execute(fato_sql, {
                    'tempo_id': tempo_id,
                    'natureza_id': natureza_id,
                    'local_id': local_id,
                    'ocorrencia_hora': row['OCORRENCIA_HORA']
                })
            connection.commit()
            print(f"   -> Carregado {len(df)} linhas na tabela 'OCORRENCIA'.")
    except Exception as e:
        print(f"   ❌ Erro fatal ao processar {csv_url}. Rollback da transação: {e}")
        try:
            connection.rollback()
        except:
            pass
        return
# ---------------------------------------------------------------------

def get_or_create_dimension(connection, table_name, lookup_cols, data):
    """
    Realiza o 'Lookup' (busca) em uma tabela Dimensão. 
    Se o registro existir, retorna o ID. Se não, insere e retorna o novo ID.
    """
    
    # 1. Monta a condição de busca
    where_clauses = [f"{col} = :{col}" for col in lookup_cols]
    where_sql = " AND ".join(where_clauses)
    lookup_col_id = f"{table_name.lower()}_id"
    
    # 2. Busca o ID existente
    select_sql = f"SELECT {lookup_col_id} FROM public.{table_name} WHERE {where_sql}"
    lookup_data = {col: data.get(col) for col in lookup_cols}
    
    result = connection.execute(text(select_sql), lookup_data).fetchone()

    if result:
        return result[0]
    
    # 3. Se não existe, insere e retorna o novo ID
    insert_cols = ", ".join(lookup_cols)
    insert_values = ", ".join([f":{col}" for col in lookup_cols])
    
    insert_sql = f"INSERT INTO public.{table_name} ({insert_cols}) VALUES ({insert_values}) RETURNING {lookup_col_id}"
    
    try:
        new_id = connection.execute(text(insert_sql), lookup_data).scalar_one()
        return new_id
    except IntegrityError:
        # Tenta buscar novamente em caso de concorrência (raro, mas necessário)
        connection.rollback()
        result = connection.execute(text(select_sql), lookup_data).fetchone()
        if result:
            return result[0]
        else:
            raise Exception(f"Erro de concorrência: {table_name}")

def load_csv_to_relational_db(raw_dir, engine):
    """
    Carrega os dados dos CSVs para o modelo relacional (TEMPO, NATUREZA, LOCAL, OCORRENCIA).
    """
    print("Função de ETL antiga desativada. Novo fluxo será em memória.")
    return


# --- FUNÇÃO PRINCIPAL ---

def main():

    # 1. Conexão com o banco de dados
    DB_URL = os.environ.get("DATABASE_URL")
    if DB_URL is None:
        print("="*60)
        print("ERRO: A variável de ambiente 'DATABASE_URL' não foi definida.")
        print("Por favor, defina a URL de conexão do seu Supabase (PostgreSQL) nos Secrets.")
        print("="*60)
        sys.exit(1)
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        print("Conexão com o Supabase (PostgreSQL) estabelecida com sucesso!")
    except ProgrammingError as e:
        print(f"❌ ERRO DE CONEXÃO: Verifique a URL e se as tabelas (TEMPO, NATUREZA, LOCAL, OCORRENCIA) foram criadas no Supabase. Detalhes: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ ERRO INESPERADO: {e}")
        sys.exit(1)

    # 2. Obter links antigos (2016-2024)
    old_links = get_old_csv_links(URL_PORTAL)
    # 3. Obter links de 2025
    links_2025 = get_2025_csv_links()
    # 4. Unir todos os links
    all_links = old_links + links_2025
    if not all_links:
        print("\n⚠️ Nenhuma URL de download encontrada. Nada será processado.")
        return

    # 5. Processar cada CSV diretamente da internet
    print("\n🔽 Iniciando pipeline de ingestão direta dos arquivos antigos e 2025...")
    for url in all_links:
        process_csv_url_to_db(url, engine)
        time.sleep(0.5)
    print("\n✅ Pipeline de ingestão concluído.")


if __name__ == "__main__":
    main()