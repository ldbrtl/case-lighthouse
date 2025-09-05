WITH src AS (

    SELECT
        cod_transacao,
        num_conta,
        data_transacao,
        nome_transacao,
        cast(valor_transacao AS numeric) AS valor_transacao          
                
    FROM {{ source('raw', 'transacoes') }}

)


SELECT * FROM src