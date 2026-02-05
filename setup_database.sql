-- 1. Cria o banco de dados (se não existir) e seleciona ele
CREATE DATABASE IF NOT EXISTS crimes_db 
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;

USE crimes_db;

-- 2. Limpeza: Remove tabelas antigas se existirem (Ordem correta para não quebrar FKs)
DROP TABLE IF EXISTS OCORRENCIA;
DROP TABLE IF EXISTS TEMPO;
DROP TABLE IF EXISTS NATUREZA;
DROP TABLE IF EXISTS LOCAL;

-- -----------------------------------------------------
-- 3. Tabela Dimensão: TEMPO
-- Armazena as datas para evitar repetição
-- -----------------------------------------------------
CREATE TABLE TEMPO (
    tempo_id INT AUTO_INCREMENT PRIMARY KEY,
    data_completa DATE NOT NULL,
    ocorrencia_ano INT,
    ocorrencia_mes INT,
    ocorrencia_dia_semana VARCHAR(50),
    ocorrencia_periodo VARCHAR(50),
    
    -- Índice único para garantir que não duplicamos datas no ETL
    UNIQUE KEY idx_tempo_data (data_completa)
) ENGINE=InnoDB;

-- -----------------------------------------------------
-- 4. Tabela Dimensão: NATUREZA
-- Armazena os tipos de crime e descrições
-- -----------------------------------------------------
CREATE TABLE NATUREZA (
    natureza_id INT AUTO_INCREMENT PRIMARY KEY,
    natureza1_descricao VARCHAR(255),
    natureza2_descricao VARCHAR(255),
    tipo_envolvimento VARCHAR(100),
    
    -- Índice para agilizar a busca "get_or_create" do Python
    INDEX idx_natureza_busca (natureza1_descricao, natureza2_descricao, tipo_envolvimento)
) ENGINE=InnoDB;

-- -----------------------------------------------------
-- 5. Tabela Dimensão: LOCAL
-- Armazena endereços, bairros e regionais
-- -----------------------------------------------------
CREATE TABLE LOCAL (
    local_id INT AUTO_INCREMENT PRIMARY KEY,
    bairro_nome VARCHAR(100),
    regional_nome VARCHAR(100),
    logradouro_nome VARCHAR(255),
    classificacao_bairro_regional VARCHAR(100),
    
    -- Índice para agilizar a busca "get_or_create" do Python
    INDEX idx_local_busca (bairro_nome, regional_nome, logradouro_nome)
) ENGINE=InnoDB;

-- -----------------------------------------------------
-- 6. Tabela Fato: OCORRENCIA
-- Liga tudo e armazena o evento
-- -----------------------------------------------------
CREATE TABLE OCORRENCIA (
    ocorrencia_id INT AUTO_INCREMENT PRIMARY KEY,
    tempo_id INT NOT NULL,
    natureza_id INT NOT NULL,
    local_id INT NOT NULL,
    ocorrencia_hora VARCHAR(10), -- Mantemos varchar pois as vezes vem "10:30" as vezes "NULL"
    
    -- Chaves Estrangeiras (FK) conectando às dimensões
    CONSTRAINT fk_ocorrencia_tempo 
        FOREIGN KEY (tempo_id) REFERENCES TEMPO (tempo_id)
        ON DELETE CASCADE,
        
    CONSTRAINT fk_ocorrencia_natureza 
        FOREIGN KEY (natureza_id) REFERENCES NATUREZA (natureza_id)
        ON DELETE CASCADE,
        
    CONSTRAINT fk_ocorrencia_local 
        FOREIGN KEY (local_id) REFERENCES LOCAL (local_id)
        ON DELETE CASCADE
) ENGINE=InnoDB;

-- Confirmação
SELECT 'Tabelas criadas com sucesso!' AS status;