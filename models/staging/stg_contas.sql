with src as (

    select
        num_conta,
        cod_cliente,
        cod_agencia,
        cod_colaborador,
        tipo_conta,
        data_abertura,
        cast(saldo_total as numeric) as saldo_total,
        cast(saldo_disponivel as numeric) as saldo_disponivel,
        data_ultimo_lancamento        
                
    from {{ source('raw', 'contas') }}

)


select * from src