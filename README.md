# 🚨 Análise de Ocorrências Criminais em Curitiba (2016-2025)

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.0+-orange.svg)](https://www.mysql.com/)
[![Power BI](https://img.shields.io/badge/Power%20BI-Desktop-yellow.svg)](https://powerbi.microsoft.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 📊 Sobre o Projeto

Este projeto realiza uma análise completa das **ocorrências criminais em Curitiba** de 2016 a 2025, utilizando dados abertos fornecidos pela Prefeitura Municipal. O objetivo é identificar padrões temporais, áreas de maior risco e fornecer insights acionáveis para segurança pública.

### 🎯 Objetivos:
- ✅ Coletar e processar dados de múltiplas fontes (2016-2025)
- ✅ Criar um modelo dimensional (Star Schema) no MySQL
- ✅ Realizar análise exploratória de dados (EDA)
- ✅ Desenvolver dashboards interativos no Power BI
- ✅ Identificar padrões e tendências criminais

---

## 🏗️ Arquitetura do Projeto

```
┌─────────────────────────────────────────────────┐
│  FONTES DE DADOS (Web Scraping)                 │
│  • Portal UFPR (2016-2024)                      │
│  • Portal Dados Abertos Curitiba (2025)        │
└──────────────────┬──────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────┐
│  ETL PYTHON (coleta_mysql.py)                   │
│  • Coleta automática de CSVs                    │
│  • Limpeza e transformação                      │
│  • Carga no modelo dimensional                  │
└──────────────────┬──────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────┐
│  BANCO DE DADOS MySQL (LOCAL)                   │
│  ┌──────────────────────────────────────────┐  │
│  │  MODELO DIMENSIONAL (STAR SCHEMA)        │  │
│  │  ┌──────────┐    ┌──────────────┐       │  │
│  │  │DIM_TEMPO │───▶│FATO_OCORRENCIA│◀────┐│  │
│  │  └──────────┘    └──────────────┘      ││  │
│  │  ┌──────────┐            ▲             ││  │
│  │  │DIM_LOCAL │────────────┘             ││  │
│  │  └──────────┘                          ││  │
│  │  ┌───────────┐                         ││  │
│  │  │DIM_NATUREZA│─────────────────────────┤│  │
│  │  └───────────┘                          │  │
│  │  ┌─────────┐                            │  │
│  │  │DIM_HORA │────────────────────────────┘  │
│  │  └─────────┘                               │
│  └──────────────────────────────────────────┘  │
└──────────────────┬──────────────────────────────┘
                   │
          ┌────────┴────────┐
          ▼                 ▼
┌──────────────────┐  ┌──────────────────┐
│  ANÁLISE         │  │  VISUALIZAÇÃO    │
│  • Jupyter       │  │  • Power BI      │
│  • Pandas        │  │  • Dashboards    │
│  • SQL Queries   │  │  • Relatórios    │
└──────────────────┘  └──────────────────┘
```

---

## 🚀 Início Rápido

### Pré-requisitos:
- MySQL 8.0+ instalado
- Python 3.11+ instalado
- Power BI Desktop (opcional)

### Instalação em 5 passos:

```bash
# 1. Clone o repositório
git clone https://github.com/seu-usuario/crimes-curitiba.git
cd crimes-curitiba

# 2. Configure o banco de dados
mysql -u root -p < setup_database.sql

# 3. Crie um ambiente virtual Python
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# 4. Instale as dependências
pip install -r requirements.txt

# 5. Configure suas credenciais MySQL no arquivo coleta_mysql.py
# Edite: DB_CONFIG = {'user': 'root', 'password': 'SUA_SENHA', ...}

# 6. Execute a coleta de dados
python coleta_mysql.py
```

**📖 Para instruções detalhadas, consulte:** [SETUP.md](SETUP.md)

---

## 📁 Estrutura do Projeto

```
crimes-curitiba/
│
├── 📄 setup_database.sql          # Script de criação do banco dimensional
├── 🐍 coleta_mysql.py             # Script de coleta e carga de dados
├── 📊 consultas_uteis.sql         # Queries SQL prontas para análise
├── 📝 requirements.txt            # Dependências Python
├── 📖 SETUP.md                    # Guia completo de instalação
├── 📋 README.md                   # Este arquivo
├── .gitignore                     # Arquivos ignorados pelo Git
│
├── 📂 notebooks/
│   └── analise_dados.ipynb        # Análise exploratória (Jupyter)
│
├── 📂 data/                       # ⚠️ NÃO VERSIONADO (ignorado)
│   ├── raw/                       # CSVs baixados automaticamente
│   └── processed/                 # Dados processados
│
└── 📂 .devcontainer/
    └── devcontainer.json          # Configuração do ambiente Dev
```

---

## 🗄️ Modelo de Dados

### Tabelas Dimensão:

| Tabela | Descrição | Registros Típicos |
|--------|-----------|-------------------|
| `DIM_TEMPO` | Datas das ocorrências | ~3.650 (10 anos) |
| `DIM_NATUREZA` | Tipos de crime | ~500-1.000 |
| `DIM_LOCAL` | Bairros e regionais | ~1.500-2.000 |
| `DIM_HORA` | Horários das ocorrências | 1.440 (minutos do dia) |

### Tabela Fato:

| Tabela | Descrição | Registros Típicos |
|--------|-----------|-------------------|
| `FATO_OCORRENCIA` | Ocorrências criminais | ~3-5 milhões |

### Views Criadas:

- ✅ `vw_ocorrencias_completas` - JOIN completo de todas dimensões
- ✅ `vw_crimes_por_ano` - Agregação anual
- ✅ `vw_top_bairros` - Ranking de bairros
- ✅ `vw_crimes_por_periodo` - Distribuição por período do dia

---

## 📊 Análises Disponíveis

### 1. Temporal
- Evolução anual de crimes (2016-2025)
- Sazonalidade mensal
- Padrões por dia da semana
- Distribuição por período do dia

### 2. Geográfica
- Top 20 bairros com mais ocorrências
- Análise por regional
- Mapas de calor

### 3. Por Tipo de Crime
- Ranking dos crimes mais frequentes
- Evolução temporal por categoria
- Análise de categorias agregadas

### 4. Cruzada
- Crime × Dia da semana
- Crime × Período do dia
- Bairro × Tipo de crime

---

## 🔍 Principais Insights

> **Nota:** Execute `python coleta_mysql.py` e depois as análises para obter insights atualizados.

Exemplos de insights que podem ser extraídos:

- 📈 **Tendência Temporal:** Identificação de anos com pico de criminalidade
- 🗺️ **Geografia de Risco:** Bairros com maior concentração de crimes
- ⏰ **Horários Críticos:** Períodos do dia mais perigosos
- 📅 **Padrões Semanais:** Dias da semana com maior incidência
- 🔍 **Tipos Prevalentes:** Crimes mais comuns por região

---

## 💻 Consultas SQL Úteis

```sql
-- Total de ocorrências por ano
SELECT 
    ocorrencia_ano,
    COUNT(*) AS total
FROM vw_ocorrencias_completas
GROUP BY ocorrencia_ano
ORDER BY ocorrencia_ano;

-- Top 10 bairros mais perigosos
SELECT 
    bairro_nome,
    COUNT(*) AS total_ocorrencias,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM FATO_OCORRENCIA), 2) AS percentual
FROM vw_ocorrencias_completas
GROUP BY bairro_nome
ORDER BY total_ocorrencias DESC
LIMIT 10;

-- Distribuição por período do dia
SELECT 
    periodo_dia,
    COUNT(*) AS total,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentual
FROM vw_ocorrencias_completas
GROUP BY periodo_dia;
```

**📖 Mais consultas em:** [consultas_uteis.sql](consultas_uteis.sql)

---

## 📊 Dashboard Power BI

### Como conectar:

1. Abra o Power BI Desktop
2. "Obter Dados" → "MySQL database"
3. Servidor: `localhost`, Banco: `crimes_curitiba`
4. Importe as views: `vw_ocorrencias_completas`, etc.

### Visualizações sugeridas:

- 📈 Gráfico de linha: Evolução anual
- 📊 Gráfico de barras: Top bairros
- 🔢 Cartões: KPIs principais
- 🗓️ Matriz: Crime × Dia da semana
- 🕐 Gráfico de rosca: Distribuição por período

**🔗 Dashboard publicado:** *(adicione o link após publicar)*

---

## 🛠️ Tecnologias Utilizadas

### Backend & ETL:
- ![Python](https://img.shields.io/badge/Python-3.11-blue) - Linguagem principal
- ![Pandas](https://img.shields.io/badge/Pandas-2.0-green) - Manipulação de dados
- ![BeautifulSoup](https://img.shields.io/badge/BeautifulSoup4-4.12-orange) - Web scraping
- ![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-red) - ORM

### Banco de Dados:
- ![MySQL](https://img.shields.io/badge/MySQL-8.0-blue) - Banco relacional

### Visualização:
- ![Power BI](https://img.shields.io/badge/Power%20BI-Desktop-yellow) - Dashboards
- ![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-orange) - Análise exploratória
- ![Matplotlib](https://img.shields.io/badge/Matplotlib-3.7-blue) - Gráficos Python

---

## 📚 Fontes de Dados

1. **Portal de Dados Abertos UFPR (2016-2024):**
   - https://dadosabertos.c3sl.ufpr.br/curitiba/Sigesguarda/

2. **Portal de Dados Abertos Curitiba (2025):**
   - https://dadosabertos.curitiba.pr.gov.br/conjuntodado/detalhe?chave=b16ead9d-835e-41e8-a4d7-dcc4f2b4b627

**Formato:** CSV com separador `;` (ponto e vírgula)

**Atualização:** Mensal (dados de 2025 ainda em coleta)

---

## 🤝 Como Contribuir

Contribuições são bem-vindas! Siga estes passos:

1. Fork este repositório
2. Crie uma branch: `git checkout -b minha-feature`
3. Commit suas mudanças: `git commit -m 'Adiciona nova feature'`
4. Push para a branch: `git push origin minha-feature`
5. Abra um Pull Request

---

## 📝 Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

---

## 👤 Autor

**Seu Nome**
- GitHub: [@seu-usuario](https://github.com/seu-usuario)
- LinkedIn: [Seu Nome](https://linkedin.com/in/seu-perfil)

---

## 🙏 Agradecimentos

- Prefeitura Municipal de Curitiba - Dados Abertos
- UFPR - Portal de Dados
- Comunidade Open Source

---

## 📞 Suporte

Encontrou algum problema? Tem sugestões?

- 📧 Email: seu.email@exemplo.com
- 🐛 Issues: [GitHub Issues](https://github.com/seu-usuario/crimes-curitiba/issues)

---

## 🔄 Atualizações Futuras

- [ ] Análise preditiva com Machine Learning
- [ ] API REST para consulta de dados
- [ ] Mapas interativos com Folium
- [ ] Integração com dados meteorológicos
- [ ] Dashboard web com Streamlit

---

**⭐ Se este projeto foi útil, considere dar uma estrela!**
