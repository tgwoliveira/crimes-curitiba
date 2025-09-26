import pandas as pd
import os
import requests
from bs4 import BeautifulSoup

# --- Configurações de URL ---
# Fonte 1: Histórico (índice de arquivos)
BASE_URL_HISTORICO = "http://dadosabertos.c3sl.ufpr.br/curitiba/Sigesguarda/"
# Fonte 2: Mais Recente (página com links em tabela)
URL_DADOS_MAIS_RECENTES = "https://dadosabertos.curitiba.pr.gov.br/conjuntodado/detalhe?chave=b16ead9d-835e-41e8-a4d7-dcc4f2b4b627"
# ----------------------------

# Pasta de saída (apenas a pasta RAW é necessária)
RAW_DIR = "data/raw"
os.makedirs(RAW_DIR, exist_ok=True)


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


def get_latest_links_from_table():
    """Busca links de arquivos CSV na tabela do portal mais recente (2024/2025)."""
    print("🌐 Buscando links na tabela do portal de dados mais recentes...")
    recent_links = []
    try:
        response = requests.get(URL_DADOS_MAIS_RECENTES)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Procuramos por links que contenham o padrão de nome de arquivo CSV
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            if href and 'Base_de_Dados.csv' in href and 'mid-dadosabertos' in href:
                recent_links.append(href)
        
        return recent_links

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao acessar o portal mais recente: {e}")
        return []


# =========================================================
# ETAPA 1: RASPAR TODAS AS FONTES DE DADOS
# =========================================================

# 1. Coleta Histórica
csv_links = get_historic_links()
print(f"✅ {len(csv_links)} arquivos CSV encontrados na fonte histórica (2016-2023).")

# 2. Coleta Recente
recent_links = get_latest_links_from_table()
for link in recent_links:
    if link not in csv_links:
        csv_links.append(link)

print(f"✅ {len(recent_links)} arquivos CSV encontrados na fonte recente (2024/2025).")

if not csv_links:
    print("⚠️ NENHUM link CSV válido encontrado em nenhuma fonte.")
    exit()

# Ordenar a lista para garantir o processamento em ordem cronológica
csv_links.sort() 
print(f"\n✅ Total de {len(csv_links)} arquivos únicos a serem processados.")


# =========================================================
# ETAPA 2: BAIXAR E SALVAR ARQUIVOS INDIVIDUAIS EM data/raw/
# =========================================================
print("\n🔽 Iniciando download incremental...")

for download_url in csv_links:
    # Cria um nome de arquivo seguro a partir da URL
    filename = download_url.split('/')[-1].split('?')[0] 
    raw_path = os.path.join(RAW_DIR, filename)
    
    # Se o arquivo já existe, pulamos o download
    if os.path.exists(raw_path):
        print(f"   -> Pulando: {filename} (já existe)")
        continue
        
    print(f"   -> Baixando: {filename}")

    try:
        # 1. Download
        response = requests.get(download_url, stream=True)
        response.raise_for_status()
        
        # 2. Salvar o arquivo bruto
        with open(raw_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"      ✅ Salvo em {raw_path}")

    except requests.exceptions.HTTPError as e:
        print(f"      ❌ Erro HTTP ao processar {filename}: {e}")
    except Exception as e:
        print(f"      ❌ Erro inesperado ao processar {filename}: {e}")

# --- FIM DA COLETA ---
print("\n✅ Pipeline de download concluído. Arquivos individuais prontos para análise no Notebook.")