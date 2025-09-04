WITH

transacoes AS (
  SELECT
        cod_transacao,
        cod_agencia,
        CAST(data_transacao AS DATE) as dt,
        valor_transacao,
        nome_transacao
  FROM {{ ref('fct_transacoes') }}
),


por_dia AS (
  SELECT
    d.day_of_week          AS day_of_week,
    d.day_of_week_name     AS day_of_week_name,
    t.dt      AS date_day,
    count(*)               AS qtd_transacoes_dia,
    SUM(ABS(t.valor_transacao))      AS valor_total_dia
  FROM transacoes t
  JOIN {{ ref('dim_dates') }} d
    ON t.dt = d.date_day
  GROUP BY
    day_of_week, day_of_week_name, date_day
),


analysis AS (
  SELECT
    day_of_week,
    day_of_week_name,
    AVG(qtd_transacoes_dia)  AS media_qtd_transacoes,
    AVG(valor_total_dia)     AS media_valor_movimentado
  FROM por_dia
  GROUP BY day_of_week, day_of_week_name
)

SELECT
  *
FROM analysis
ORDER BY day_of_week
