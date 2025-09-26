import pandas as pd
import os

# URL de exemplo (ajustar para o dataset correto da prefeitura)
url = "https://dadosabertos.curitiba.pr.gov.br/dataset/ocorrencias-criminais.csv"

# Pasta de saída
os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

# Baixar os dados
print("🔽 Baixando dados...")
df = pd.read_csv(url, sep=";", encoding="utf-8", low_memory=False)
print(f"✅ Dados coletados: {len(df)} linhas")

# Salvar dados brutos
df.to_csv("data/raw/ocorrencias.csv", index=False, encoding="utf-8")

# --- Tratamento inicial ---
print("🧹 Tratando dados...")
if "data_ocorrencia" in df.columns:
    df["data"] = pd.to_datetime(df["data_ocorrencia"], errors="coerce")
    df["ano"] = df["data"].dt.year
    df["mes"] = df["data"].dt.month

# Salvar tratados
df.to_csv("data/processed/ocorrencias_tratadas.csv", index=False, encoding="utf-8")
print("✅ Dados tratados salvos em data/processed/ocorrencias_tratadas.csv")
