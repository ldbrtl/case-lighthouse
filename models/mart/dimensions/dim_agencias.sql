with src as (
    select
    
    cod_agencia,
    nome,
    tipo_agencia,
    endereco,
    cidade,
    uf,
    data_abertura

    from {{ ref('stg_agencias') }}


)

select * from src

