WITH src AS (

    SELECT
        cod_colaborador,
        primeiro_nome,
        ultimo_nome,
        CONCAT(primeiro_nome, ' ', ultimo_nome) AS nome_colaborador,
        email,
        data_nascimento,
        endereco,
        cep
             
                
    FROM {{ ref('stg_colaboradores') }}

)

SELECT * FROM src