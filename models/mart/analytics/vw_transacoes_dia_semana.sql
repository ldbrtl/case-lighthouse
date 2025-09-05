WITH


transacoes AS (
  SELECT
    CAST(data_transacao AS DATE) AS dt,
    ABS(valor_transacao)         AS valor_transacao
  FROM {{ ref('fct_transacoes') }}
),


base AS (
  SELECT
    d.day_of_week,
    d.day_of_week_name,
    t.dt AS date_day,
    t.valor_transacao
  FROM transacoes t
  JOIN {{ ref('dim_dates') }} d
    ON t.dt = d.date_day
),


por_dia AS (
  SELECT
    day_of_week,
    day_of_week_name,
    date_day,
    COUNT(*)                 AS qtd_transacoes_dia,
    SUM(valor_transacao)     AS valor_total_dia
  FROM base
  GROUP BY 1,2,3
),


medias AS (
  SELECT
    day_of_week,
    day_of_week_name,
    AVG(qtd_transacoes_dia) AS media_qtd_transacoes,
    AVG(valor_total_dia)    AS media_valor_movimentado
  FROM por_dia
  GROUP BY 1,2
)


SELECT
  *
FROM medias
ORDER BY day_of_week
