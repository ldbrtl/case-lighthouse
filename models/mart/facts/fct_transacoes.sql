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

    from {{ ref('stg_transacoes')}} as t
    left join 
        {{ ref('stg_contas')}} as c
        on 
        t.num_conta = c.num_conta

)

select * from src
