WITH src AS (

    SELECT
        cod_colaborador,
        cod_agencia        

    FROM {{ source('raw', 'colaborador_agencia') }}

)


SELECT * FROM src