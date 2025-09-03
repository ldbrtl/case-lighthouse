with src as (

    select
        cod_transacao,
        num_conta,
        data_transacao,
        nome_transacao,
        cast(valor_transacao as numeric) as valor_transacao          
                
    from {{ source('raw', 'transacoes') }}

)


select * from src