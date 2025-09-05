with 

transacoes as (
    select 
        cod_transacao,
        cod_agencia,
        CAST(data_transacao AS DATE) as dt,
        ABS(valor_transacao) AS valor_transacao,
        nome_transacao
    from 
        {{ ref('fct_transacoes') }}
),

period as (
    select 
        MAX(dt) as max_dt,
        DATE_SUB(MAX(dt), INTERVAL 6 MONTH) as min_dt
    from transacoes
),

valid_dates as (
    select
        date_day
    from 
        {{ ref('dim_dates') }}
    where 
        date_day between (select min_dt from period) and (select max_dt from period)
),

base as (
  select
    t.cod_agencia,
    a.nome as agencia_nome,
    a.uf as agencia_uf,
    a.tipo_agencia,
    t.valor_transacao
  from transacoes t
  join valid_dates d
    on t.dt = d.date_day
  join {{ ref('dim_agencias') }} a
    on t.cod_agencia = a.cod_agencia
),

grouped as (
  select
    cod_agencia,
    agencia_nome,
    agencia_uf,
    tipo_agencia,
    count(*) as qtd_transacoes,
    sum(valor_transacao) as valor_total,
    avg(valor_transacao) as ticket_medio
  from base
  group by
    cod_agencia, agencia_nome, agencia_uf, tipo_agencia
),

stats as (
  select
    avg(qtd_transacoes) as media_qtd,
    sum(qtd_transacoes) as total_qtd
  from grouped
),

source_data as (
  select
    gr.*,
    round( SAFE_DIVIDE(gr.qtd_transacoes, s.total_qtd), 4 )       as pct_do_total,
    case when gr.qtd_transacoes >= s.media_qtd then 1 else 0 end  as acima_da_media,
    dense_rank() over (order by gr.qtd_transacoes desc)           as rk_desc,
    dense_rank() over (order by gr.qtd_transacoes asc)            as rk_asc
  from grouped gr
  cross join stats s
)

select
  *
from
  source_data
order by
  qtd_transacoes desc
