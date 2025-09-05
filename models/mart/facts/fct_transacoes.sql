with src as (
    select
    t.cod_transacao,
    t.num_conta,
    t.data_transacao,
    t.nome_transacao,
    t.valor_transacao,
    c.tipo_conta,
    c.cod_cliente,
    c.cod_agencia,
    c.tipo_agencia,
    c.nome
 
    from {{ ref('stg_transacoes')}} as t
    left join 
        {{ ref('dim_contas')}} as c
        on 
        t.num_conta = c.num_conta

)

select * from src
