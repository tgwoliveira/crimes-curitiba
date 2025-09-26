import pandas as pd
import os
import requests
from bs4 import BeautifulSoup

# --- Configurações da Base de Dados ---
# Fonte 1: Histórico (índice de arquivos)
BASE_URL_HISTORICO = "http://dadosabertos.c3sl.ufpr.br/curitiba/Sigesguarda/"
# Fonte 2: Mais Recente (página com link de download)
URL_DADOS_MAIS_RECENTES = "https://dadosabertos.curitiba.pr.gov.br/conjuntodado/detalhe?chave=b16ead9d-835e-41e8-a4d7-dcc4f2b4b627"
# -------------------------------------

# Pastas de saída
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

FINAL_CSV_PATH = os.path.join(PROCESSED_DIR, "ocorrencias_tratadas.csv")
primeiro_arquivo = True 


def get_historic_links():
    """Busca links de arquivos CSV no portal histórico."""
    csv_links = []
    try:
        response = requests.get(BASE_URL_HISTORICO)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        for link in soup.find_all('a'):
            href = link.get('href')
            if href and href.endswith('.csv') and 'sigesguarda' in href:
                csv_links.append(BASE_URL_HISTORICO + href)
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao acessar o portal histórico: {e}")
    return csv_links


def get_latest_link():
    """Busca o link de download direto na página mais recente."""
    print("🌐 Buscando link no portal de dados mais recentes...")
    try:
        response = requests.get(URL_DADOS_MAIS_RECENTES)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # O link de download geralmente tem a classe 'btn btn-primary' ou um atributo específico.
        # Procuramos por um link que termine em .csv dentro da página.
        # Ajuste esta lógica se não funcionar, mas esta é a tentativa mais robusta:
        for link in soup.find_all('a'):
            href = link.get('href')
            if href and 'ocorrencias-criminais.csv' in href and 'download' in href:
                # O link direto é o valor do href
                return href
        
        # Se não encontrar o link direto, tenta outra heurística comum de portais de dados abertos
        for link in soup.find_all('a', class_='btn-primary'):
            href = link.get('href')
            if href and 'ocorrencias-criminais' in href:
                return href
                
        print("⚠️ Link de download CSV não encontrado na página mais recente.")
        return None

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao acessar o portal mais recente: {e}")
        return None


# =========================================================
# ETAPA 1: RASPAR TODAS AS FONTES DE DADOS
# =========================================================

# 1. Coleta Histórica
csv_links = get_historic_links()
print(f"✅ {len(csv_links)} arquivos CSV encontrados na fonte histórica.")

# 2. Coleta Recente
latest_link = get_latest_link()
if latest_link and latest_link not in csv_links:
    csv_links.append(latest_link)
    print("✅ Link mais recente adicionado para processamento.")

if not csv_links:
    print("⚠️ NENHUM link CSV válido encontrado em nenhuma fonte.")
    exit()

# Ordenar a lista para garantir o processamento em ordem cronológica
csv_links.sort() 
print(f"\n✅ Total de {len(csv_links)} arquivos únicos a serem processados.")


# =========================================================
# ETAPA 2: BAIXAR, TRATAR E SALVAR INCREMENTALMENTE
# =========================================================
print("\n🔽 Iniciando processamento incremental...")
total_linhas = 0

for download_url in csv_links:
    # A URL mais recente pode ser longa, pegamos o último segmento para nome de exibição
    filename = download_url.split('/')[-1].split('?')[0] 
    raw_path = os.path.join(RAW_DIR, filename)
    
    print(f"   -> Processando: {filename}")

    try:
        # 1. Download e Leitura
        response = requests.get(download_url, stream=True)
        response.raise_for_status()
        with open(raw_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        # Leitura: Usando encoding="latin-1" para evitar erro de codificação
        df = pd.read_csv(raw_path, sep=";", encoding="latin-1", low_memory=False)
        
        # 2. Tratamento e Renomeação (Aplicado a cada DF individual)
        COLUNAS_PARA_RENOMEAR = {
            'OCORRENCIA_ANO': 'ano',                     
            'NATUREZA1_DESCRICAO': 'tipo_crime',       
            'ATENDIMENTO_BAIRRO_NOME': 'bairro',        
            'OCORRENCIA_DATA': 'data_completa'          
        }
        df.rename(columns=COLUNAS_PARA_RENOMEAR, inplace=True)

        if 'data_completa' in df.columns:
            # OTIMIZAÇÃO: Define o formato exato para acelerar a conversão
            df["data"] = pd.to_datetime(df["data_completa"], errors="coerce", format='%d/%m/%Y %H:%M:%S')
            df["mes"] = df["data"].dt.month

        # 3. Escrita Incremental no Arquivo Final (Otimização de Memória)
        header_flag = primeiro_arquivo # Não precisa de 'global' aqui
        mode_flag = 'w' if primeiro_arquivo else 'a'
        
        df.to_csv(
            FINAL_CSV_PATH, 
            mode=mode_flag, 
            header=header_flag, 
            index=False, 
            encoding="utf-8"
        )
        
        total_linhas += len(df)
        print(f"      ✅ Adicionado ({len(df)} linhas). Total acumulado: {total_linhas} linhas.")
        
        primeiro_arquivo = False

    except requests.exceptions.HTTPError as e:
        print(f"      ❌ Erro HTTP ao processar {filename}: {e}")
    except Exception as e:
        print(f"      ❌ Erro inesperado ao processar {filename}: {e}")

# --- FIM DA COLETA E PROCESSAMENTO ---
print(f"\n✅ Pipeline concluído. Total de dados coletados e salvos: {total_linhas} linhas.")