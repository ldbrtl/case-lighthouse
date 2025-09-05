WITH transacoes AS (
  SELECT
    DATE(data_transacao) AS data_transacao,         
    ABS(valor_transacao) AS valor_abs,
    cod_transacao
  FROM {{ ref('fct_transacoes') }}

),
dolar AS (
  SELECT date_day, cotacao_dolar
  FROM {{ ref('dim_dolar') }}
)
SELECT
  d.cotacao_dolar,
  COUNT(*) AS qtd_transacoes,
  SUM(t.valor_abs) AS total_transacoes,
  AVG(t.valor_abs) AS valor_medio_transacao
FROM transacoes t
JOIN dolar d
  ON t.data_transacao = d.date_day        
GROUP BY d.cotacao_dolar
ORDER BY d.cotacao_dolar
