with src as (

    select
        cod_agencia,
        nome,
        endereco,
        cidade,
        uf,
        data_abertura,
        tipo_agencia

    from {{ source('raw', 'agencias') }}

)


select * from src