with src as (

    select
        cod_cliente,
        primeiro_nome,
        ultimo_nome,
        email,
        tipo_cliente,
        data_inclusao,
        regexp_replace(cpfcnpj, r'[^0-9]', '') as cpf_cnpj,
        data_nascimento,
        endereco,
        regexp_replace(cep,     r'[^0-9]', '') as cep

    from {{ source('raw', 'clientes') }}

)


select * from src