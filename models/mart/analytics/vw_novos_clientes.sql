WITH

inclusao_cliente AS (
  SELECT 
  cod_cliente, 
  CAST(data_inclusao AS DATE) AS dt_inclusao
  FROM 
    {{ ref('dim_clientes') }}
)

, cliente_agencia AS (
  SELECT DISTINCT cod_cliente, cod_agencia
  FROM {{ ref('stg_contas') }}

)


SELECT
  d.year_number    AS ano,
  d.month_of_year  AS mes_num,
  d.month_name     AS mes_nome,
  ca.cod_agencia,
  a.nome,
  a.uf,
  a.tipo_agencia,
  COUNT(DISTINCT ic.cod_cliente) AS novos_clientes

FROM inclusao_cliente ic

JOIN {{ ref('dim_dates') }} d
  ON ic.dt_inclusao = d.date_day

JOIN cliente_agencia ca
  ON ca.cod_cliente = ic.cod_cliente

LEFT JOIN {{ ref('dim_agencias') }} a
  ON a.cod_agencia = ca.cod_agencia

GROUP BY ano, mes_num, mes_nome, ca.cod_agencia, a.nome, a.uf, a.tipo_agencia
ORDER BY ano, mes_num, cod_agencia
