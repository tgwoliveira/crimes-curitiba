-- =====================================================
-- CONSULTAS ÚTEIS - Análise de Crimes Curitiba
-- =====================================================
-- Use estas queries no MySQL Workbench ou como referência
-- para criar visualizações no Power BI

USE crimes_curitiba;

-- =====================================================
-- 1. ESTATÍSTICAS GERAIS
-- =====================================================

-- Total de ocorrências por ano
SELECT 
    ocorrencia_ano,
    COUNT(*) AS total_ocorrencias,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentual
FROM vw_ocorrencias_completas
GROUP BY ocorrencia_ano
ORDER BY ocorrencia_ano;

-- Total geral de ocorrências
SELECT COUNT(*) AS total_registros FROM FATO_OCORRENCIA;

-- Período coberto pelos dados
SELECT 
    MIN(data_completa) AS data_inicial,
    MAX(data_completa) AS data_final,
    DATEDIFF(MAX(data_completa), MIN(data_completa)) AS dias_cobertos
FROM DIM_TEMPO;

-- =====================================================
-- 2. ANÁLISE TEMPORAL
-- =====================================================

-- Ocorrências por mês (tendência mensal)
SELECT 
    ocorrencia_ano,
    nome_mes,
    COUNT(*) AS total_ocorrencias
FROM vw_ocorrencias_completas
GROUP BY ocorrencia_ano, ocorrencia_mes, nome_mes
ORDER BY ocorrencia_ano, ocorrencia_mes;

-- Ocorrências por dia da semana
SELECT 
    ocorrencia_dia_semana,
    COUNT(*) AS total_ocorrencias,
    ROUND(AVG(CASE WHEN ocorrencia_ano >= 2020 THEN 1 ELSE 0 END) * COUNT(*), 0) AS media_2020_mais
FROM vw_ocorrencias_completas
GROUP BY ocorrencia_dia_semana
ORDER BY 
    FIELD(ocorrencia_dia_semana, 'DOMINGO', 'SEGUNDA-FEIRA', 'TERÇA-FEIRA', 
          'QUARTA-FEIRA', 'QUINTA-FEIRA', 'SEXTA-FEIRA', 'SÁBADO');

-- Ocorrências por período do dia
SELECT 
    periodo_dia,
    COUNT(*) AS total_ocorrencias,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentual
FROM vw_ocorrencias_completas
GROUP BY periodo_dia
ORDER BY 
    FIELD(periodo_dia, 'MADRUGADA', 'MANHÃ', 'TARDE', 'NOITE');

-- =====================================================
-- 3. ANÁLISE GEOGRÁFICA
-- =====================================================

-- Top 20 bairros com mais ocorrências
SELECT 
    bairro_nome,
    regional_nome,
    COUNT(*) AS total_ocorrencias,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM FATO_OCORRENCIA), 2) AS percentual
FROM vw_ocorrencias_completas
GROUP BY bairro_nome, regional_nome
ORDER BY total_ocorrencias DESC
LIMIT 20;

-- Ocorrências por regional
SELECT 
    regional_nome,
    COUNT(*) AS total_ocorrencias,
    COUNT(DISTINCT bairro_nome) AS numero_bairros
FROM vw_ocorrencias_completas
WHERE regional_nome IS NOT NULL
GROUP BY regional_nome
ORDER BY total_ocorrencias DESC;

-- Evolução temporal por bairro (Top 5)
SELECT 
    bairro_nome,
    ocorrencia_ano,
    COUNT(*) AS total_ocorrencias
FROM vw_ocorrencias_completas
WHERE bairro_nome IN (
    SELECT bairro_nome 
    FROM vw_ocorrencias_completas 
    GROUP BY bairro_nome 
    ORDER BY COUNT(*) DESC 
    LIMIT 5
)
GROUP BY bairro_nome, ocorrencia_ano
ORDER BY bairro_nome, ocorrencia_ano;

-- =====================================================
-- 4. ANÁLISE POR TIPO DE CRIME
-- =====================================================

-- Top 15 tipos de crime
SELECT 
    tipo_crime,
    COUNT(*) AS total_ocorrencias,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM FATO_OCORRENCIA), 2) AS percentual
FROM vw_ocorrencias_completas
GROUP BY tipo_crime
ORDER BY total_ocorrencias DESC
LIMIT 15;

-- Evolução dos principais crimes ao longo dos anos
SELECT 
    ocorrencia_ano,
    tipo_crime,
    COUNT(*) AS total_ocorrencias
FROM vw_ocorrencias_completas
WHERE tipo_crime IN (
    SELECT tipo_crime 
    FROM vw_ocorrencias_completas 
    GROUP BY tipo_crime 
    ORDER BY COUNT(*) DESC 
    LIMIT 5
)
GROUP BY ocorrencia_ano, tipo_crime
ORDER BY tipo_crime, ocorrencia_ano;

-- Categorias de crime (agregado)
SELECT 
    n.categoria_crime,
    COUNT(*) AS total_ocorrencias,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM FATO_OCORRENCIA), 2) AS percentual
FROM FATO_OCORRENCIA f
JOIN DIM_NATUREZA n ON f.natureza_id = n.natureza_id
GROUP BY n.categoria_crime
ORDER BY total_ocorrencias DESC;

-- =====================================================
-- 5. ANÁLISES CRUZADAS
-- =====================================================

-- Crime por dia da semana (Top 5 crimes)
SELECT 
    ocorrencia_dia_semana,
    tipo_crime,
    COUNT(*) AS total_ocorrencias
FROM vw_ocorrencias_completas
WHERE tipo_crime IN (
    SELECT tipo_crime 
    FROM vw_ocorrencias_completas 
    GROUP BY tipo_crime 
    ORDER BY COUNT(*) DESC 
    LIMIT 5
)
GROUP BY ocorrencia_dia_semana, tipo_crime
ORDER BY 
    FIELD(ocorrencia_dia_semana, 'DOMINGO', 'SEGUNDA-FEIRA', 'TERÇA-FEIRA', 
          'QUARTA-FEIRA', 'QUINTA-FEIRA', 'SEXTA-FEIRA', 'SÁBADO'),
    total_ocorrencias DESC;

-- Crime por período do dia (Top 5 crimes)
SELECT 
    periodo_dia,
    tipo_crime,
    COUNT(*) AS total_ocorrencias
FROM vw_ocorrencias_completas
WHERE tipo_crime IN (
    SELECT tipo_crime 
    FROM vw_ocorrencias_completas 
    GROUP BY tipo_crime 
    ORDER BY COUNT(*) DESC 
    LIMIT 5
)
GROUP BY periodo_dia, tipo_crime
ORDER BY 
    FIELD(periodo_dia, 'MADRUGADA', 'MANHÃ', 'TARDE', 'NOITE'),
    total_ocorrencias DESC;

-- Bairros mais perigosos por tipo de crime (matriz)
SELECT 
    b.bairro_nome,
    c.tipo_crime,
    COUNT(*) AS total_ocorrencias
FROM vw_ocorrencias_completas b
JOIN vw_ocorrencias_completas c ON b.bairro_nome = c.bairro_nome
WHERE b.bairro_nome IN (
    SELECT bairro_nome 
    FROM vw_ocorrencias_completas 
    GROUP BY bairro_nome 
    ORDER BY COUNT(*) DESC 
    LIMIT 10
)
AND c.tipo_crime IN (
    SELECT tipo_crime 
    FROM vw_ocorrencias_completas 
    GROUP BY tipo_crime 
    ORDER BY COUNT(*) DESC 
    LIMIT 5
)
GROUP BY b.bairro_nome, c.tipo_crime
ORDER BY b.bairro_nome, total_ocorrencias DESC;

-- =====================================================
-- 6. ANÁLISES COMPARATIVAS
-- =====================================================

-- Comparação ano a ano (crescimento)
SELECT 
    ocorrencia_ano,
    COUNT(*) AS total_ocorrencias,
    LAG(COUNT(*)) OVER (ORDER BY ocorrencia_ano) AS ano_anterior,
    ROUND(
        (COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY ocorrencia_ano)) * 100.0 / 
        LAG(COUNT(*)) OVER (ORDER BY ocorrencia_ano), 
        2
    ) AS variacao_percentual
FROM vw_ocorrencias_completas
GROUP BY ocorrencia_ano
ORDER BY ocorrencia_ano;

-- Média mensal por ano
SELECT 
    ocorrencia_ano,
    ROUND(COUNT(*) / 12.0, 0) AS media_mensal,
    MAX(mes_count) AS mes_pico,
    MIN(mes_count) AS mes_menor
FROM (
    SELECT 
        ocorrencia_ano,
        ocorrencia_mes,
        COUNT(*) AS mes_count
    FROM vw_ocorrencias_completas
    GROUP BY ocorrencia_ano, ocorrencia_mes
) AS monthly
GROUP BY ocorrencia_ano
ORDER BY ocorrencia_ano;

-- =====================================================
-- 7. RANKING E TOP N
-- =====================================================

-- Top 10 combinações Bairro + Tipo de Crime
SELECT 
    bairro_nome,
    tipo_crime,
    COUNT(*) AS total_ocorrencias,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking
FROM vw_ocorrencias_completas
GROUP BY bairro_nome, tipo_crime
ORDER BY total_ocorrencias DESC
LIMIT 10;

-- Top 5 horários mais perigosos
SELECT 
    h.hora,
    COUNT(*) AS total_ocorrencias,
    h.periodo_dia
FROM FATO_OCORRENCIA f
JOIN DIM_HORA h ON f.hora_id = h.hora_id
GROUP BY h.hora, h.periodo_dia
ORDER BY total_ocorrencias DESC
LIMIT 5;

-- =====================================================
-- 8. FILTROS ÚTEIS PARA POWER BI
-- =====================================================

-- Anos disponíveis (para filtro)
SELECT DISTINCT ocorrencia_ano 
FROM vw_ocorrencias_completas 
ORDER BY ocorrencia_ano;

-- Bairros disponíveis (para filtro)
SELECT DISTINCT bairro_nome 
FROM vw_ocorrencias_completas 
ORDER BY bairro_nome;

-- Tipos de crime disponíveis (para filtro)
SELECT DISTINCT tipo_crime 
FROM vw_ocorrencias_completas 
ORDER BY tipo_crime;

-- Regionais disponíveis (para filtro)
SELECT DISTINCT regional_nome 
FROM vw_ocorrencias_completas 
WHERE regional_nome IS NOT NULL
ORDER BY regional_nome;

-- =====================================================
-- 9. VERIFICAÇÃO DE QUALIDADE DOS DADOS
-- =====================================================

-- Registros com dados faltantes
SELECT 
    'Bairro NULL' AS tipo,
    COUNT(*) AS total
FROM vw_ocorrencias_completas
WHERE bairro_nome IS NULL OR bairro_nome = 'NÃO INFORMADO'

UNION ALL

SELECT 
    'Crime NULL' AS tipo,
    COUNT(*) AS total
FROM vw_ocorrencias_completas
WHERE tipo_crime IS NULL OR tipo_crime = 'NÃO INFORMADO'

UNION ALL

SELECT 
    'Regional NULL' AS tipo,
    COUNT(*) AS total
FROM vw_ocorrencias_completas
WHERE regional_nome IS NULL;

-- Distribuição de registros por tabela dimensão
SELECT 'DIM_TEMPO' AS tabela, COUNT(*) AS registros FROM DIM_TEMPO
UNION ALL
SELECT 'DIM_NATUREZA', COUNT(*) FROM DIM_NATUREZA
UNION ALL
SELECT 'DIM_LOCAL', COUNT(*) FROM DIM_LOCAL
UNION ALL
SELECT 'DIM_HORA', COUNT(*) FROM DIM_HORA
UNION ALL
SELECT 'FATO_OCORRENCIA', COUNT(*) FROM FATO_OCORRENCIA;

-- =====================================================
-- FIM DAS CONSULTAS
-- =====================================================
