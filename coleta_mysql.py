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

# ==============================================================================
# CONFIGURAÇÕES GLOBAIS
# ==============================================================================

# ⚠️ CONFIRA SUA CONEXÃO
DB_CONNECTION_STRING = "mysql+pymysql://root:thiagohc@localhost/crimes_db"
URL_PORTAL = "https://dadosabertos.c3sl.ufpr.br/curitiba/Sigesguarda/"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': URL_PORTAL
}

CSV_COLUMNS = [
    'OCORRENCIA_DATA', 'OCORRENCIA_ANO', 'OCORRENCIA_MES', 'OCORRENCIA_HORA', 
    'OCORRENCIA_DIA_SEMANA', 'OCORRENCIA_PERIODO', 'ATENDIMENTO_BAIRRO_NOME', 
    'ATENDIMENTO_REGIONAL_NOME', 'ATENDIMENTO_LOGRADOURO_NOME', 
    'CLASSIFICACAO_BAIRRO_REGIONAL', 'NATUREZA1_CODIGO', 'NATUREZA1_DESCRICAO', 
    'NATUREZA2_DESCRICAO', 'TIPO_ENVOLVIMENTO', 'ATENDIMENTO_NUMERO' 
]

# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================

def limpar_valor(valor):
    if pd.isna(valor): return None
    s_val = str(valor).strip()
    if s_val == '' or s_val.lower() in ['nan', 'none']: return None
    return s_val.upper()

def get_old_csv_links(portal_url):
    print(f"🌐 Buscando links na página do portal: {portal_url}")
    try:
        response = requests.get(portal_url, headers=HEADERS)
        response.raise_for_status() 
        soup = BeautifulSoup(response.text, 'html.parser')
        links = set()
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            if href and '.csv' in href.lower() and ('ocorrencias-criminais' in href.lower() or 'base_de_dados' in href.lower()):
                links.add(urljoin(portal_url, href))
        old_links = [l for l in links if 'mid-dadosabertos' not in l]
        old_links.sort()
        print(f"✅ Encontrados {len(old_links)} links.")
        return old_links
    except Exception as e:
        print(f"❌ Erro ao acessar o portal: {e}")
        return []

# ==============================================================================
# FUNÇÕES DE BANCO DE DADOS (CORRIGIDA)
# ==============================================================================

def get_or_create_dimension(connection, table_name, lookup_subset_cols, full_data):
    """
    lookup_subset_cols: Lista de colunas usadas APENAS para verificar se existe (ex: só a data).
    full_data: Dicionário com TODOS os dados para inserir caso não exista.
    """
    
    # 1. Tenta buscar usando apenas as colunas chaves (ex: data_completa)
    where_clauses = [f"{col} = :{col}" for col in lookup_subset_cols]
    where_sql = " AND ".join(where_clauses)
    lookup_col_id = f"{table_name.lower()}_id"
    
    select_sql = f"SELECT {lookup_col_id} FROM {table_name} WHERE {where_sql}"
    
    # Filtra o dicionário de dados apenas para o SELECT
    lookup_values = {k: full_data[k] for k in lookup_subset_cols}

    try:
        result = connection.execute(text(select_sql), lookup_values).fetchone()
        if result:
            return result[0]
        
        # 2. Se não achou, insere com TODOS os dados
        cols = list(full_data.keys())
        insert_cols_str = ", ".join(cols)
        insert_vals_str = ", ".join([f":{col}" for col in cols])
        
        insert_sql = f"INSERT INTO {table_name} ({insert_cols_str}) VALUES ({insert_vals_str})"
        
        connection.execute(text(insert_sql), full_data)
        
        # Recupera o ID gerado
        new_id = connection.execute(text("SELECT LAST_INSERT_ID()")).scalar()
        return new_id

    except IntegrityError:
        # Se der erro de duplicidade (race condition ou dados inconsistentes), tenta buscar de novo
        # Isso resolve o problema de: "O Select não achou, mas o Insert disse que já existe"
        result = connection.execute(text(select_sql), lookup_values).fetchone()
        if result:
            return result[0]
        else:
            # Se falhar aqui, é um erro real de banco (ex: violação de outra constraint)
            raise

# ==============================================================================
# PROCESSAMENTO PRINCIPAL
# ==============================================================================

def process_csv_url_to_db(csv_url, engine):
    nome_arquivo = csv_url.split('/')[-1]
    print(f"\n🌐 Processando CSV: {nome_arquivo}")
    
    try:
        chunks = pd.read_csv(csv_url, sep=";", encoding="latin1", low_memory=False, dtype=str, chunksize=5000, on_bad_lines='skip')
        total_inserido = 0

        for i, df in enumerate(chunks):
            # Filtros básicos
            cols_ok = [c for c in CSV_COLUMNS if c in df.columns]
            df = df[cols_ok]
            if 'OCORRENCIA_ANO' in df.columns:
                df = df[pd.to_numeric(df['OCORRENCIA_ANO'], errors='coerce').notna()]
            
            # Tratamento de Data
            if 'OCORRENCIA_DATA' in df.columns:
                df['OCORRENCIA_DATA'] = pd.to_datetime(df['OCORRENCIA_DATA'], dayfirst=True, errors='coerce')
                df = df.dropna(subset=['OCORRENCIA_DATA'])
                df['OCORRENCIA_DATA'] = df['OCORRENCIA_DATA'].dt.strftime('%Y-%m-%d')

            with engine.connect() as connection:
                trans = connection.begin()
                try:
                    count_chunk = 0
                    for index, row in df.iterrows():
                        
                        # --- 1. TEMPO (Correção Crítica: Busca APENAS pela data) ---
                        tempo_data = {
                            'data_completa': row.get('OCORRENCIA_DATA'),
                            'ocorrencia_ano': row.get('OCORRENCIA_ANO'),
                            'ocorrencia_mes': row.get('OCORRENCIA_MES'),
                            'ocorrencia_dia_semana': limpar_valor(row.get('OCORRENCIA_DIA_SEMANA')),
                            'ocorrencia_periodo': limpar_valor(row.get('OCORRENCIA_PERIODO')),
                        }
                        # A mágica aqui: passamos ['data_completa'] como chave de busca, 
                        # mas tempo_data inteiro para inserir se precisar.
                        tempo_id = get_or_create_dimension(connection, 'TEMPO', ['data_completa'], tempo_data)

                        # --- 2. NATUREZA ---
                        natureza_data = {
                            'natureza1_descricao': limpar_valor(row.get('NATUREZA1_DESCRICAO')) or 'NAO INFORMADO',
                            'natureza2_descricao': limpar_valor(row.get('NATUREZA2_DESCRICAO')),
                            'tipo_envolvimento': limpar_valor(row.get('TIPO_ENVOLVIMENTO')),
                        }
                        natureza_id = get_or_create_dimension(connection, 'NATUREZA', ['natureza1_descricao', 'natureza2_descricao', 'tipo_envolvimento'], natureza_data)

                        # --- 3. LOCAL ---
                        local_data = {
                            'bairro_nome': limpar_valor(row.get('ATENDIMENTO_BAIRRO_NOME')) or 'NAO INFORMADO',
                            'regional_nome': limpar_valor(row.get('ATENDIMENTO_REGIONAL_NOME')),
                            'logradouro_nome': limpar_valor(row.get('ATENDIMENTO_LOGRADOURO_NOME')),
                            'classificacao_bairro_regional': limpar_valor(row.get('CLASSIFICACAO_BAIRRO_REGIONAL')),
                        }
                        # Usamos bairro, regional e logradouro como chave composta
                        local_id = get_or_create_dimension(connection, 'LOCAL', ['bairro_nome', 'regional_nome', 'logradouro_nome'], local_data)

                        # --- 4. FATO ---
                        connection.execute(text("""
                            INSERT INTO OCORRENCIA (tempo_id, natureza_id, local_id, ocorrencia_hora)
                            VALUES (:tempo_id, :natureza_id, :local_id, :ocorrencia_hora)
                        """), {
                            'tempo_id': tempo_id,
                            'natureza_id': natureza_id,
                            'local_id': local_id,
                            'ocorrencia_hora': row.get('OCORRENCIA_HORA')
                        })
                        count_chunk += 1
                    
                    trans.commit()
                    total_inserido += count_chunk
                    print(f"   ... Lote {i+1} processado: {count_chunk} inseridos.")
                
                except Exception as e:
                    trans.rollback()
                    print(f"   ⚠️ Erro no lote {i+1}: {e}")

        print(f"✅ Arquivo finalizado. Total: {total_inserido}")

    except Exception as e:
        print(f"❌ Erro fatal no arquivo: {e}")

def main():
    try:
        engine = create_engine(DB_CONNECTION_STRING)
        with engine.connect() as conn: conn.execute(text("SELECT 1"))
        print("🔌 Conexão OK!")
    except Exception as e:
        print(f"❌ Erro conexão: {e}"); return

    for url in get_old_csv_links(URL_PORTAL):
        process_csv_url_to_db(url, engine)

if __name__ == "__main__":
    main()