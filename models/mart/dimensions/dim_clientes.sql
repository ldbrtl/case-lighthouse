with src as (
    select
    
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

    from {{ ref('stg_clientes') }}

)

select * from src

