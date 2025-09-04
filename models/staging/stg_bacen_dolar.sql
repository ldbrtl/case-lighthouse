WITH src AS (

    SELECT
        CAST(data AS date)       as date_day,
        CAST(valor AS numeric) as cotacao_dolar

    FROM {{ ref('bacen_dolar') }}

)

SELECT * FROM src