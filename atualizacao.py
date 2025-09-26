import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# --- Configurações de URL e Pastas ---
# URL da página que contém a tabela com os links mais recentes (a que você enviou)
URL_DADOS_MAIS_RECENTES = "https://dadosabertos.curitiba.pr.gov.br/conjuntodado/detalhe?chave=b16ead9d-835e-41e8-a4d7-dcc4f2b4b627"

PROCESSED_DIR = "data/processed"
FINAL_CSV_PATH = os.path.join(PROCESSED_DIR, "ocorrencias_tratadas.csv")
os.makedirs(PROCESSED_DIR, exist_ok=True)

# Mapeamento de colunas (CRUCIAL para o tratamento)
COLUNAS_PARA_RENOMEAR = {
    'OCORRENCIA_ANO': 'ano',                     
    'NATUREZA1_DESCRICAO': 'tipo_crime',       
    'ATENDIMENTO_BAIRRO_NOME': 'bairro',        
    'OCORRENCIA_DATA': 'data_completa'          
}

# =========================================================
# FUNÇÕES DE COLETA E PREPARAÇÃO
# =========================================================

def get_latest_processed_date():
    """Lê o CSV final para encontrar a data mais recente processada."""
    if os.path.exists(FINAL_CSV_PATH):
        try:
            # Lendo apenas a coluna 'data' para economizar memória
            df_existing = pd.read_csv(
                FINAL_CSV_PATH, 
                usecols=['data'], 
                encoding="utf-8", 
                parse_dates=['data']
            )
            return df_existing['data'].max()
        except Exception as e:
            print(f"⚠️ Erro ao ler data do CSV existente: {e}")
            return datetime(2016, 1, 1)
    return datetime(2016, 1, 1)


def get_most_recent_download_link():
    """Raspa a tabela do novo portal e retorna o link do arquivo mais novo (primeiro da lista)."""
    print("🌐 Buscando link mais recente na tabela do portal...")
    
    try:
        response = requests.get(URL_DADOS_MAIS_RECENTES)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # CORREÇÃO: Procuramos por todas as tags <a> cujo href contenha "Base_de_Dados.csv"
        # O link mais recente geralmente é o primeiro item da tabela (ou o último link encontrado)
        
        latest_link = None
        
        # A nova URL de download começa com 'https://mid-dadosabertos.curitiba.pr.gov.br/'
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            # Filtra links que são arquivos CSV de Base de Dados
            if href and 'Base_de_Dados.csv' in href:
                # O loop irá encontrar todos, mas a estrutura da tabela HTML coloca o mais novo no topo.
                # Como a tabela HTML lista os arquivos em ordem decrescente de data (2025-06-25 é o primeiro), 
                # o primeiro link que encontrarmos será o mais novo.
                latest_link = href
                break # Pega o primeiro e sai do loop

        if latest_link:
            return latest_link
        else:
            print("⚠️ Link de Base de Dados mais recente não encontrado na tabela.")
            return None

    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao acessar o portal mais recente: {e}")
        return None


# =========================================================
# FLUXO PRINCIPAL DE ATUALIZAÇÃO
# =========================================================

print("🔄 Iniciando rotina de atualização de dados...")

# 1. Encontra a data mais recente no arquivo atual
data_limite = get_latest_processed_date()
print(f"Data mais recente no arquivo: {data_limite.strftime('%Y-%m-%d %H:%M:%S')}")


# 2. Coleta o link da fonte mais recente
download_url = get_most_recent_download_link()

if not download_url:
    print("❌ Falha ao obter o link de download. Encerrando.")
    exit()

filename = download_url.split('/')[-1]
print(f"   -> Baixando arquivo: {filename}")

try:
    # 3. Baixar e Ler o arquivo mais recente
    response = requests.get(download_url, stream=True)
    response.raise_for_status()
    
    # Lemos o arquivo diretamente do stream (mais eficiente)
    df_new = pd.read_csv(
        response.raw, 
        sep=";", 
        encoding="latin-1", 
        low_memory=False
    )
    
    # 4. Tratamento e Renomeação
    df_new.rename(columns=COLUNAS_PARA_RENOMEAR, inplace=True)
    
    # Converte e cria a coluna 'data' com o formato correto
    df_new['data'] = pd.to_datetime(
        df_new["data_completa"], 
        errors="coerce", 
        format='%d/%m/%Y %H:%M:%S'
    )
    df_new['mes'] = df_new['data'].dt.month
    
    # 5. Filtragem (o Coração da atualização)
    # Filtra apenas registros mais novos que o último registro do seu arquivo final
    df_novos_registros = df_new[df_new['data'] > data_limite]
    
    if df_novos_registros.empty:
        print("✅ Arquivo baixado, mas não há novos registros desde a última atualização. Tudo OK.")
        exit()

    # 6. Salvar apenas os novos dados (modo append)
    df_novos_registros.to_csv(
        FINAL_CSV_PATH, 
        mode='a', 
        header=False, 
        index=False, 
        encoding="utf-8"
    )

    print(f"✅ Atualização concluída! {len(df_novos_registros)} novos registros adicionados.")

except requests.exceptions.RequestException as e:
    print(f"❌ Erro de rede ao baixar o arquivo: {e}")
except Exception as e:
    print(f"❌ Erro no processamento do arquivo: {e}")