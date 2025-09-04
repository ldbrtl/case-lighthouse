WITH src AS (
    SELECT
    
    cod_cliente,
    primeiro_nome,
    ultimo_nome,
    email,
    tipo_cliente,
    data_inclusao,
    cpf_cnpj,
    data_nascimento,
    endereco,
    cep

    FROM {{ ref('stg_clientes') }}

)

SELECT * FROM src

