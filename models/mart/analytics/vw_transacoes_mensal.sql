WITH

transacoes AS (
  SELECT
    CAST(data_transacao AS DATE) AS dt,
    cod_agencia,
    ABS(valor_transacao)         AS valor_transacao
  FROM {{ ref('fct_transacoes') }}
),

por_mes_agencia AS (
  SELECT
    DATE_TRUNC(d.date_day, MONTH)      AS date_month, 
    d.year_number                      AS ano,
    d.month_of_year                    AS mes_num,
    d.month_name                       AS mes_nome,
    t.cod_agencia,
    COUNT(*)                           AS volume_transacoes,
    SUM(t.valor_transacao)             AS valor_total_transacoes,
    AVG(t.valor_transacao)             AS valor_medio_transacoes,
  FROM transacoes t
  JOIN {{ ref('dim_dates') }} d
    ON t.dt = d.date_day
  GROUP BY date_month, ano, mes_num, mes_nome, t.cod_agencia
),

source_data AS (
  SELECT
    m.date_month,
    m.ano,
    m.mes_num,
    m.mes_nome,
    m.cod_agencia,
    a.nome,
    a.uf,
    a.tipo_agencia,
    m.volume_transacoes,
    m.valor_total_transacoes,
    m.valor_medio_transacoes,
  FROM por_mes_agencia m
  LEFT JOIN {{ ref('dim_agencias') }} a
    ON m.cod_agencia = a.cod_agencia
)

SELECT *
FROM source_data
ORDER BY ano, mes_num, cod_agencia