WITH

propostas AS (
  SELECT
    *
  FROM {{ ref('fct_propostas_credito') }}
),

valid_dates AS (
  SELECT
    date_day,
    month_name,
    year_number
  FROM {{ ref('dim_dates') }}
),

perfil AS (
  SELECT
    DATE_TRUNC(d.date_day, MONTH) AS date_month_year,
    a.nome AS agencia,
    a.tipo_agencia,
    col.nome_colaborador,
    COUNT(*) AS qtd_propostas_aprovadas,
    AVG(p.valor_proposta) AS media_valor_proposta,
    SUM(p.valor_proposta) AS soma_valor_proposta,
    AVG(SAFE_DIVIDE(p.valor_financiamento, NULLIF(p.valor_proposta, 0))) AS media_pct_financiado,
    AVG(SAFE_DIVIDE(p.valor_entrada, NULLIF(p.valor_proposta, 0))) AS media_pct_entrada,
    AVG(p.taxa_juros_mensal) AS media_taxa_juros_mensal,
    AVG(p.quantidade_parcelas) AS media_quantidade_parcelas
  FROM propostas p
  JOIN {{ ref('dim_agencias') }} a
    ON p.cod_agencia_colaborador = a.cod_agencia
  JOIN {{ ref('dim_colaboradores') }} col
    ON p.cod_colaborador = col.cod_colaborador
  JOIN valid_dates d
    ON CAST(p.data_entrada_proposta AS DATE) = d.date_day
  WHERE p.status_proposta = 'Aprovada'
  GROUP BY
    DATE_TRUNC(d.date_day, MONTH),
    a.nome,
    a.tipo_agencia,
    col.nome_colaborador
),

source_data AS (
  SELECT
    d.month_name,
    d.year_number,
    m.agencia,
    m.tipo_agencia,
    m.nome_colaborador,
    m.qtd_propostas_aprovadas,
    m.media_valor_proposta,
    m.soma_valor_proposta,
    m.media_pct_financiado,
    m.media_pct_entrada,
    m.media_taxa_juros_mensal,
    m.media_quantidade_parcelas,
    m.date_month_year
  FROM perfil m
  JOIN {{ ref('dim_dates') }} d
    ON m.date_month_year = d.date_day
)

SELECT
  month_name,
  year_number,
  agencia,
  tipo_agencia,
  nome_colaborador,
  qtd_propostas_aprovadas,
  media_valor_proposta,
  soma_valor_proposta,
  media_pct_financiado,
  media_pct_entrada,
  media_taxa_juros_mensal,
  media_quantidade_parcelas
FROM source_data
ORDER BY year_number, date_month_year, agencia, nome_colaborador
