with src as (

    select
        cod_proposta,
        cod_cliente,
        cod_colaborador,
        data_entrada_proposta,
        cast(taxa_juros_mensal as numeric) as taxa_juros_mensal,
        cast(valor_proposta as numeric) as valor_proposta,
        cast(valor_financiamento as numeric) as valor_financiamento,
        cast(valor_entrada as numeric) as valor_entrada,
        cast(valor_prestacao as numeric) as valor_prestacao,
        quantidade_parcelas,
        carencia,
        status_proposta              
                
    from {{ source('raw', 'propostas_credito') }}

)


select * from src