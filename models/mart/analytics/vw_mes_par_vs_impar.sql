WITH

transacoes AS (
  SELECT
    CAST(data_transacao AS DATE) AS dt,
    ABS(valor_transacao)         AS valor_transacao
  FROM {{ ref('fct_transacoes') }}
),

por_mes AS (
  SELECT
    d.year_number      AS ano,
    d.month_of_year    AS mes_num,
    d.month_name       AS mes_nome,
    SAFE_CAST(MOD(d.month_of_year, 2) = 0 AS BOOL) AS mes_par,
    COUNT(*)                 AS qtd_transacoes_mes,
    SUM(t.valor_transacao)   AS valor_total_mes,
    AVG(t.valor_transacao)   AS ticket_medio_mes
  FROM transacoes t
  JOIN {{ ref('dim_dates') }} d
    ON t.dt = d.date_day
  GROUP BY ano, mes_num, mes_nome, mes_par
),

resumo AS (
  SELECT
    mes_par,
    AVG(qtd_transacoes_mes) AS media_qtd_transacoes_mes,
    AVG(valor_total_mes)    AS media_valor_total_mes,
    AVG(ticket_medio_mes)   AS media_ticket_medio_mes,
    COUNT(*)                AS meses_contabilizados
  FROM por_mes
  GROUP BY mes_par
),

source_data AS (
  SELECT
    CASE WHEN mes_par THEN 'par' ELSE 'impar' END AS grupo_mes,
    media_qtd_transacoes_mes,
    media_valor_total_mes,
    media_ticket_medio_mes,
    meses_contabilizados
  FROM resumo
)

SELECT
  *
FROM
  source_data
ORDER BY
  grupo_mes
