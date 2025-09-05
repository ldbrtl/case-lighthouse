WITH src AS (

    SELECT
        cod_colaborador,
        primeiro_nome,
        ultimo_nome,
        email,
        regexp_replace(cpf, r'[^0-9]', '') AS cpf,
        data_nascimento,
        endereco,
        regexp_replace(cep, r'[^0-9]', '') AS cep     
                
    FROM {{ source('raw', 'colaboradores') }}

)


SELECT * FROM src