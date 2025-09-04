WITH src AS (

    SELECT
        cod_cliente,
        primeiro_nome,
        ultimo_nome,
        email,
        tipo_cliente,
        data_inclusao,
        regexp_replace(cpfcnpj, r'[^0-9]', '') AS cpf_cnpj,
        data_nascimento,
        endereco,
        regexp_replace(cep,     r'[^0-9]', '') AS cep

    FROM {{ source('raw', 'clientes') }}

)


SELECT * FROM src