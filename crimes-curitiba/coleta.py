import pandas as pd
import os
import requests
from bs4 import BeautifulSoup

# --- Configurações da Base de Dados ---
BASE_URL = "http://dadosabertos.c3sl.ufpr.br/curitiba/Sigesguarda/"
# -------------------------------------

# Pastas de saída
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

all_data = []

# =========================================================
# ETAPA 1: RASPAR TODOS OS LINKS .CSV DO DIRETÓRIO
# =========================================================

try:
    response = requests.get(BASE_URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Encontra todos os links <a> na página
    all_links = soup.find_all('a')
    
    csv_links = []
    # Filtra links que são arquivos CSV da base de dados
    for link in all_links:
        href = link.get('href')
        # Verifica se o link existe, termina com .csv e contém o identificador "sigesguarda"
        if href and href.endswith('.csv') and 'sigesguarda' in href:
            csv_links.append(BASE_URL + href)

except requests.exceptions.RequestException as e:
    print(f"❌ Erro ao acessar a URL base ou analisar o HTML: {e}")
    exit()

if not csv_links:
    print("⚠️ NENHUM link CSV válido encontrado. Verifique se o BASE_URL está correto e se o HTML é analisável.")
    exit()

print(f"✅ {len(csv_links)} arquivos CSV encontrados para download.")

# =========================================================
# ETAPA 2: BAIXAR E UNIFICAR OS ARQUIVOS
# =========================================================
print("\n🔽 Iniciando download e unificação de dados...")

for download_url in csv_links:
    # Obtém o nome do arquivo a partir da URL
    filename = download_url.split('/')[-1]
    raw_path = os.path.join(RAW_DIR, filename)
    
    print(f"   -> Baixando: {filename}")

    try:
        # Tenta baixar o arquivo
        response = requests.get(download_url, stream=True)
        response.raise_for_status()

        # Salva o arquivo bruto
        with open(raw_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        # Carrega o CSV, unifica e lida com o separador e encoding
        # Nota: Usamos 'sep=";"' e 'encoding="utf-8"' baseado no seu código anterior.
        df_temp = pd.read_csv(raw_path, sep=";", encoding="utf-8", low_memory=False)
        all_data.append(df_temp)
        
        # os.remove(raw_path) # Descomente se quiser deletar os arquivos brutos após unificar
        print(f"      ✅ Adicionado ({len(df_temp)} linhas)")

    except requests.exceptions.HTTPError as e:
        print(f"      ❌ Erro HTTP ao baixar {filename}: {e}")
    except Exception as e:
        print(f"      ❌ Erro inesperado ao processar {filename}: {e}")
        
# --- FIM DA COLETA E UNIFICAÇÃO ---
if all_data:
    df = pd.concat(all_data, ignore_index=True)
    print(f"\n✅ Total de dados coletados e unificados: {len(df)} linhas")
else:
    print("\n⚠️ Não foi possível coletar dados de nenhum arquivo.")
    exit()

# --- Tratamento inicial ---
print("🧹 Tratando dados...")

# Mantemos o tratamento de data para o dataframe unificado
if "data_ocorrencia" in df.columns:
    df["data"] = pd.to_datetime(df["data_ocorrencia"], errors="coerce")
    df["ano"] = df["data"].dt.year
    df["mes"] = df["data"].dt.month
else:
    print("⚠️ Coluna 'data_ocorrencia' não encontrada. Verifique o cabeçalho do CSV.")

# Salvar tratados
df.to_csv(os.path.join(PROCESSED_DIR, "ocorrencias_tratadas.csv"), index=False, encoding="utf-8")
print("✅ Dados tratados e unificados salvos com sucesso!")