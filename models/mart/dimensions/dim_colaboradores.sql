with src as (

    select
        cod_colaborador,
        primeiro_nome,
        ultimo_nome,
        email,
        data_nascimento,
        endereco,
        cep
             
                
    from {{ ref('stg_colaboradores') }} AS co

)

select * from src