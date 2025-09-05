WITH

transacoes AS (
  SELECT
    *
  FROM
    {{ ref('fct_transacoes')}}
)

, valid_dates AS (
  SELECT
    *
  FROM
    {{ ref('dim_dates')}}
  WHERE date_day >= (SELECT MIN(CAST(data_transacao AS DATE)) FROM transacoes)
)

, mes_ano as (
  SELECT
    DATE_TRUNC(d.date_day, MONTH) AS date_month_year
    , COUNT(DISTINCT t.cod_cliente) AS clientes_ativos
    , t.cod_agencia
    , t.tipo_agencia
  FROM
    transacoes t
  JOIN
    valid_dates d
    ON CAST(t.data_transacao as DATE) = d.date_day
  GROUP BY
    date_month_year, cod_agencia, tipo_agencia
  ORDER BY
    date_month_year
)

, source_data as(
  SELECT
     d.month_name
    , d.year_number
    , m.clientes_ativos
    , m.cod_agencia
    , a.nome
    , m.tipo_agencia
  FROM
    mes_ano m
  LEFT JOIN
    {{ ref('dim_agencias')}} a
    ON m.cod_agencia = a.cod_agencia
  LEFT JOIN
    {{ ref('dim_dates')}} d
    ON m.date_month_year = d.date_day
  ORDER BY
  d.year_number, d.month_of_year
)

SELECT
  *
FROM
  source_data