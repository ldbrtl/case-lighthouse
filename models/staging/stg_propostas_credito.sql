WITH src AS (

    SELECT
        cod_proposta,
        cod_cliente,
        cod_colaborador,
        data_entrada_proposta,
        cast(taxa_juros_mensal AS numeric) AS taxa_juros_mensal,
        cast(valor_proposta AS numeric) AS valor_proposta,
        cast(valor_financiamento AS numeric) AS valor_financiamento,
        cast(valor_entrada AS numeric) AS valor_entrada,
        cast(valor_prestacao AS numeric) AS valor_prestacao,
        quantidade_parcelas,
        carencia,
        status_proposta              
                
    FROM {{ source('raw', 'propostas_credito') }}

)


SELECT * FROM src