WITH src AS (

    SELECT
        cod_agencia,
        nome,
        endereco,
        cidade,
        uf,
        data_abertura,
        tipo_agencia

    FROM {{ source('raw', 'agencias') }}

)


SELECT * FROM src