with transacoes as (
  select 
    cast(t.data_transacao as date) as data_transacao,
    t.valor_transacao
  from {{ ref('fct_transacoes') }} t
),
dolar as (
  select 
    date_day,
    cotacao_dolar
  from {{ ref('dim_dolar') }}
)
select
  d.cotacao_dolar,
  count(*) as qtd_transacoes,
  sum(t.valor_transacao) as total_transacoes,
  avg(t.valor_transacao) as valor_medio_transacao
from transacoes t
left join dolar d
  on t.data_transacao = d.date_day
group by d.cotacao_dolar
order by d.cotacao_dolar
