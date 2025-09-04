WITH src AS (

    SELECT
        num_conta,
        cod_cliente,
        cod_agencia,
        cod_colaborador,
        tipo_conta,
        data_abertura,
        cast(saldo_total AS numeric) AS saldo_total,
        cast(saldo_disponivel AS numeric) AS saldo_disponivel,
        data_ultimo_lancamento        
                
    FROM {{ source('raw', 'contas') }}

)


SELECT * FROM src