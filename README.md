# 🚨 Análise de Ocorrências Criminais em Curitiba 2016 - 2025 

## 💡 Sobre o Projeto

Este projeto tem como objetivo principal coletar, processar e analisar dados públicos de ocorrências criminais fornecidos pela Prefeitura de Curitiba. O objetivo é identificar **padrões temporais, áreas de maior risco** e fornecer uma base de dados limpa para futuras análises de segurança pública.

O ambiente de desenvolvimento é totalmente reproduzível via GitHub Codespaces.

Fonte: [text](https://dadosabertos.curitiba.pr.gov.br/conjuntodado/detalhe?chave=b16ead9d-835e-41e8-a4d7-dcc4f2b4b627)

## 🛠️ Tecnologias e Ambiente

Todo o ambiente de trabalho está configurado no Codespaces para garantir a reprodutibilidade.

* **Linguagem:** Python 3.11
* **Bibliotecas de Dados:** `pandas`, `requests`, `beautifulsoup4`
* **Análise:** Jupyter Notebook (`matplotlib` para visualização inicial)
* **Dashboard Interativo:** Power BI (Link abaixo)
* **Configuração:** `devcontainer.json` (Garante que todas as bibliotecas sejam instaladas automaticamente via `requirements.txt`).

### ⚙️ Como Rodar (Codespaces)

Basta clicar no botão **"Code"** no GitHub e selecionar **"Create codespace on main"**. O ambiente fará automaticamente:
1.  Instalação das dependências (`pip install -r requirements.txt`).
2.  Execução da coleta de dados (`python coleta.py`) para baixar e unificar a base de dados histórica.

---

## 📊 Análise e Resultados

Os resultados da análise exploratória de dados (EDA) estão disponíveis no **Jupyter Notebook** e no **Dashboard Interativo**.

### 1. Análise Exploratória (EDA)

Acesse o Notebook completo para ver os gráficos e as conclusões passo a passo.

➡️ **[Acesse o Notebook de Análise Completa aqui: `notebooks/analise.ipynb`](/notebooks/analise.ipynb)**

### 2. Dashboard Interativo (Power BI)

Abaixo está o dashboard interativo construído no Power BI. Utilize os filtros de ano e tipo de crime para explorar as tendências por bairro.

*(Aqui você colocaria a imagem do seu dashboard e/ou o link de incorporação)*

**[Clique aqui para ver o Dashboard Completo no Power BI Service](LINK_DO_SEU_DASHBOARD_PUBLICADO)**

---

## 🔑 Principais Insights Encontrados

Após a análise dos dados de [ANO INICIAL] a [ANO FINAL], identificamos:

* **Tendência Temporal:** [Exemplo: Houve um **aumento de 15%** no número de ocorrências de **furtos** nos anos de 2023 e 2024, após um período de estabilidade.]
* **Geografia de Risco:** [Exemplo: Os bairros **Centro** e **Sítio Cercado** representam, juntos, mais de **25%** do total de ocorrências registradas no período.]
* **Foco por Dia da Semana:** [Exemplo: Crimes de **roubo** atingem pico nas **noites de sexta-feira**, enquanto ocorrências de **dano ao patrimônio** são mais distribuídas durante a semana.]

---

## 📁 Estrutura do Repositório

* `coleta.py` → Script para raspar e unificar os dados brutos da UFPR.
* `data/raw/` → Dados brutos coletados (ignorados pelo Git, mas gerados pelo script).
* `data/processed/` → Contém o arquivo `ocorrencias_tratadas.csv` unificado.
* `notebooks/` → Contém o `analise.ipynb` com a EDA e visualizações.
* `.devcontainer/` → Arquivo de configuração para o Codespaces.