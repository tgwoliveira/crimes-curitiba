# 🚀 GUIA COMPLETO DE CONFIGURAÇÃO - Análise de Crimes Curitiba

## 📋 Índice

1. [Pré-requisitos](#pré-requisitos)
2. [Instalação do MySQL](#instalação-do-mysql)
3. [Configuração do Banco de Dados](#configuração-do-banco-de-dados)
4. [Instalação do Python e Dependências](#instalação-do-python-e-dependências)
5. [Execução da Coleta de Dados](#execução-da-coleta-de-dados)
6. [Configuração do Power BI](#configuração-do-power-bi)
7. [Resolução de Problemas](#resolução-de-problemas)

---

## 🔧 Pré-requisitos

### Software Necessário:

- ✅ **MySQL Community Server 8.0+** (ou MariaDB 10.5+)
- ✅ **MySQL Workbench** (interface visual - opcional mas recomendado)
- ✅ **Python 3.11+**
- ✅ **Power BI Desktop** (gratuito)
- ✅ **Git** (para clonar o repositório)

---

## 📦 1. Instalação do MySQL

### Windows:

1. **Download do MySQL:**
   - Acesse: https://dev.mysql.com/downloads/installer/
   - Baixe o "MySQL Installer for Windows"

2. **Instalação:**
   - Execute o instalador
   - Escolha: "Developer Default" ou "Server only"
   - Configure uma senha para o usuário `root` (ANOTE ESSA SENHA!)
   - Porta padrão: 3306
   - Opcionalmente, instale o MySQL Workbench

3. **Verificar instalação:**
   ```cmd
   mysql --version
   ```

### Linux (Ubuntu/Debian):

```bash
# Atualizar repositórios
sudo apt update

# Instalar MySQL Server
sudo apt install mysql-server

# Instalar MySQL Workbench (opcional)
sudo apt install mysql-workbench

# Iniciar o serviço
sudo systemctl start mysql
sudo systemctl enable mysql

# Configurar senha do root
sudo mysql_secure_installation
```

### macOS:

```bash
# Usando Homebrew
brew install mysql

# Iniciar o serviço
brew services start mysql

# Configurar senha
mysql_secure_installation
```

---

## 🗄️ 2. Configuração do Banco de Dados

### Passo 1: Acessar o MySQL

**Pelo Terminal/CMD:**
```bash
mysql -u root -p
```
Digite a senha configurada na instalação.

**Pelo MySQL Workbench:**
1. Abra o MySQL Workbench
2. Clique em "Local instance MySQL"
3. Digite a senha do root

### Passo 2: Executar o Script de Criação

**Opção A - Pelo MySQL Workbench:**
1. Clique em "File" > "Open SQL Script"
2. Selecione o arquivo `setup_database.sql`
3. Clique no ícone de raio ⚡ para executar

**Opção B - Pelo Terminal:**
```bash
# Navegue até a pasta do projeto
cd D:\DEV\PROJECTS\crimes-curitiba

# Execute o script
mysql -u root -p < setup_database.sql
```

### Passo 3: Verificar Criação

```sql
-- No MySQL Workbench ou terminal MySQL
USE crimes_curitiba;
SHOW TABLES;
```

Você deve ver 5 tabelas:
- `DIM_TEMPO`
- `DIM_NATUREZA`
- `DIM_LOCAL`
- `DIM_HORA`
- `FATO_OCORRENCIA`

---

## 🐍 3. Instalação do Python e Dependências

### Passo 1: Verificar instalação do Python

```bash
python --version
# ou
python3 --version
```

Deve mostrar Python 3.11 ou superior.

### Passo 2: Criar ambiente virtual (recomendado)

```bash
# Navegue até a pasta do projeto
cd D:\DEV\PROJECTS\crimes-curitiba

# Crie o ambiente virtual
python -m venv venv

# Ative o ambiente virtual
# Windows:
venv\Scripts\activate

# Linux/Mac:
source venv/bin/activate
```

### Passo 3: Instalar dependências

```bash
# Atualizar pip
pip install --upgrade pip

# Instalar bibliotecas necessárias
pip install -r requirements.txt

# Instalar PyMySQL (conector MySQL para Python)
pip install pymysql
```

### Passo 4: Configurar credenciais do MySQL

Edite o arquivo `coleta_mysql.py` e altere as linhas:

```python
# Configuração do banco MySQL LOCAL
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',  # ← ALTERE se usar outro usuário
    'password': 'SUA_SENHA_AQUI',  # ← COLOQUE SUA SENHA AQUI
    'database': 'crimes_curitiba'
}
```

**⚠️ IMPORTANTE:** Nunca commite senhas no Git! O `.gitignore` já está configurado para ignorar arquivos de configuração.

---

## 📥 4. Execução da Coleta de Dados

### Executar o script de coleta:

```bash
# Certifique-se de estar na pasta do projeto
cd D:\DEV\PROJECTS\crimes-curitiba

# Execute o script
python coleta_mysql.py
```

### O que vai acontecer:

1. ✅ Script conecta no MySQL local
2. ✅ Busca links dos CSVs (2016-2025) nos portais
3. ✅ Baixa e processa cada CSV **diretamente da internet**
4. ✅ Carrega dados no modelo dimensional
5. ✅ Mostra estatísticas ao final

### Tempo estimado:
- Depende da sua conexão
- Aproximadamente **30-60 minutos** para todos os anos
- Você verá o progresso em tempo real

### Após a conclusão:

Verifique os dados no MySQL:

```sql
USE crimes_curitiba;

-- Ver total de ocorrências
SELECT COUNT(*) FROM FATO_OCORRENCIA;

-- Ver período dos dados
SELECT 
    MIN(data_completa) AS data_inicial,
    MAX(data_completa) AS data_final
FROM DIM_TEMPO;

-- Chamar procedure de estatísticas
CALL sp_estatisticas_banco();
```

---

## 📊 5. Configuração do Power BI

### Passo 1: Instalar Power BI Desktop

- Download: https://powerbi.microsoft.com/desktop/
- Instale normalmente
- É gratuito!

### Passo 2: Conectar ao MySQL

1. Abra o Power BI Desktop
2. Clique em "Obter Dados" > "Mais..."
3. Procure por "MySQL database"
4. Clique em "Conectar"

**Se o conector MySQL não aparecer:**
- Baixe e instale: [MySQL Connector/Net](https://dev.mysql.com/downloads/connector/net/)
- Reinicie o Power BI

### Passo 3: Configurar conexão

```
Servidor: localhost
Banco de dados: crimes_curitiba
```

**Modo de conectividade de dados:**
- Escolha "DirectQuery" para dados sempre atualizados
- Ou "Import" para melhor performance (recomendado)

**Credenciais:**
- Usuário: root (ou seu usuário MySQL)
- Senha: sua senha MySQL

### Passo 4: Importar tabelas/views

Marque as seguintes views para importar:
- ✅ `vw_ocorrencias_completas` (principal)
- ✅ `vw_crimes_por_ano`
- ✅ `vw_top_bairros`
- ✅ `vw_crimes_por_periodo`

### Passo 5: Criar visualizações

Agora você pode criar:
- 📈 Gráficos de linha (evolução temporal)
- 📊 Gráficos de barras (ranking de bairros)
- 🗺️ Mapas de calor
- 🔢 Cartões com KPIs
- 📉 Gráficos de pizza (distribuição por tipo)

---

## 🔎 6. Análise com Jupyter Notebook

### Conectar ao MySQL no Notebook:

```python
import pandas as pd
from sqlalchemy import create_engine

# Criar conexão
engine = create_engine(
    'mysql+pymysql://root:SUA_SENHA@localhost:3306/crimes_curitiba'
)

# Ler dados
df = pd.read_sql("""
    SELECT * FROM vw_ocorrencias_completas
    WHERE ocorrencia_ano >= 2020
""", engine)

# Analisar
print(df.head())
print(df.info())
```

---

## ❗ 7. Resolução de Problemas

### Problema: "Access denied for user 'root'@'localhost'"

**Solução:**
```bash
# Resetar senha do MySQL
# Windows: Pare o serviço MySQL primeiro

# Linux/Mac:
sudo mysql
ALTER USER 'root'@'localhost' IDENTIFIED BY 'nova_senha';
FLUSH PRIVILEGES;
```

### Problema: "Can't connect to MySQL server"

**Verificar se MySQL está rodando:**

Windows:
```cmd
# Abrir Serviços (services.msc)
# Procurar por "MySQL" e verificar se está "Executando"
```

Linux:
```bash
sudo systemctl status mysql
sudo systemctl start mysql
```

### Problema: "Table doesn't exist"

**Solução:** Execute novamente o `setup_database.sql`

### Problema: PyMySQL não encontrado

```bash
pip install pymysql
```

### Problema: Power BI não conecta

1. Verifique se MySQL está rodando
2. Teste a conexão no MySQL Workbench primeiro
3. Instale o MySQL Connector/Net
4. Reinicie o Power BI

---

## 📂 Estrutura Final do Projeto

```
D:\DEV\PROJECTS\crimes-curitiba\
│
├── .git/                          # Controle de versão
├── .gitignore                     # Arquivos ignorados
├── .devcontainer/
│   └── devcontainer.json
│
├── data/                          # ⚠️ NÃO VERSIONADO
│   ├── raw/                       # CSVs baixados (gerados automaticamente)
│   └── processed/                 # Arquivos processados (se houver)
│
├── notebooks/
│   └── analise_dados.ipynb        # Análises exploratórias
│
├── setup_database.sql             # 🆕 Script de criação do banco
├── coleta_mysql.py                # 🆕 Script de coleta (versão MySQL)
├── requirements.txt               # Dependências Python
├── README.md                      # Documentação principal
└── SETUP.md                       # 🆕 Este guia
```

---

## ✅ Checklist de Configuração

- [ ] MySQL instalado e rodando
- [ ] Banco `crimes_curitiba` criado
- [ ] Tabelas criadas (5 tabelas)
- [ ] Python 3.11+ instalado
- [ ] Ambiente virtual criado
- [ ] Dependências instaladas (`pip install -r requirements.txt`)
- [ ] Senha do MySQL configurada no `coleta_mysql.py`
- [ ] Script de coleta executado com sucesso
- [ ] Dados carregados no banco (verificar com SELECT COUNT(*))
- [ ] Power BI instalado
- [ ] Power BI conectado ao MySQL
- [ ] Dashboard inicial criado

---

## 🎯 Próximos Passos

Após configurar tudo:

1. **Explorar os dados:** Abra o Jupyter Notebook
2. **Criar dashboard:** Use o Power BI
3. **Publicar:** Power BI Service (online, gratuito)
4. **Compartilhar:** Adicione o link do dashboard no README.md

---

## 📞 Suporte

Se encontrar problemas:

1. Verifique este guia completamente
2. Consulte a seção de "Resolução de Problemas"
3. Verifique os logs de erro do Python/MySQL
4. Abra uma Issue no GitHub (se aplicável)

---

**Boa sorte com sua análise! 🚀**
