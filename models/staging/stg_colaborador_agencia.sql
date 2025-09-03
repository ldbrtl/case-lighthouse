with src as (

    select
        cod_colaborador,
        cod_agencia        

    from {{ source('raw', 'colaborador_agencia') }}

)


select * from src