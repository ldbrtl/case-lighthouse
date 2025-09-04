WITH src AS (
    SELECT
    
    cod_agencia,
    nome,
    tipo_agencia,
    endereco,
    cidade,
    uf,
    data_abertura

    FROM {{ ref('stg_agencias') }}


)

SELECT * FROM src

