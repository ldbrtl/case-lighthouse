WITH

transacoes AS (
    SELECT 
        cod_transacao,
        cod_agencia,
        CAST(data_transacao AS DATE) AS dt,
        ABS(valor_transacao) AS valor_transacao,
        nome_transacao
    FROM 
        {{ ref('fct_transacoes') }}
),

period AS (
    SELECT 
        MAX(dt) AS max_dt,
        DATE_SUB(MAX(dt), INTERVAL 6 MONTH) AS min_dt
    FROM transacoes
),

valid_dates AS (
    SELECT
        date_day
    FROM 
        {{ ref('dim_dates') }}
    WHERE 
        date_day between (select min_dt from period) and (select max_dt from period)
),

base AS (
  SELECT
    t.cod_agencia,
    a.nome AS agencia_nome,
    a.uf AS agencia_uf,
    a.tipo_agencia,
    t.valor_transacao
  FROM transacoes t
  JOIN valid_dates d
    on t.dt = d.date_day
  JOIN {{ ref('dim_agencias') }} a
    on t.cod_agencia = a.cod_agencia
),

grouped AS (
  SELECT
    cod_agencia,
    agencia_nome,
    agencia_uf,
    tipo_agencia,
    count(*) AS qtd_transacoes,
    sum(valor_transacao) AS valor_total,
    avg(valor_transacao) AS ticket_medio
  FROM base
  GROUP BY
    cod_agencia, agencia_nome, agencia_uf, tipo_agencia
),

stats as (
  SELECT
    avg(qtd_transacoes) as media_qtd,
    sum(qtd_transacoes) as total_qtd
  FROM grouped
),

source_data as (
  SELECT
    gr.*,
    round( SAFE_DIVIDE(gr.qtd_transacoes, s.total_qtd), 4 ) as pct_do_total,
    case when gr.qtd_transacoes >= s.media_qtd then 1 else 0 end as acima_da_media,
    dense_rank() over (order by gr.qtd_transacoes desc) as rk_desc,
    dense_rank() over (order by gr.qtd_transacoes asc) as rk_asc
  FROM grouped gr
  cross join stats s
)

SELECT
  *
FROM
  source_data
ORDER BY
  qtd_transacoes desc
