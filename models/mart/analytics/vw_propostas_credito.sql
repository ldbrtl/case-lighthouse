WITH

propostas AS (
  SELECT *
  FROM {{ ref('fct_propostas_credito') }}
),

perfil AS (
  SELECT

    d.month_start_date AS date_month,     
    d.year_number AS year_number,
    d.month_of_year AS month_num,
    d.month_name AS month_name,
    a.nome AS agencia,
    a.tipo_agencia AS tipo_agencia,
    col.nome_colaborador AS nome_colaborador,
    COUNT(*) AS qtd_propostas_aprovadas,
    AVG(p.valor_proposta) AS media_valor_proposta,
    SUM(p.valor_proposta) AS soma_valor_proposta,

  FROM propostas p
  JOIN {{ ref('dim_agencias') }} a
    ON p.cod_agencia_colaborador = a.cod_agencia
  JOIN {{ ref('dim_colaboradores') }} col
    ON p.cod_colaborador = col.cod_colaborador
  JOIN {{ ref('dim_dates') }} d
    ON CAST(p.data_entrada_proposta AS DATE) = d.date_day
  WHERE p.status_proposta = 'Aprovada'
  GROUP BY
    date_month, year_number, month_num, month_name,
    agencia, tipo_agencia, nome_colaborador
)

SELECT
  *
FROM perfil
ORDER BY date_month, agencia, nome_colaborador
