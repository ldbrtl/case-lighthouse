with src as (

    select
        cod_colaborador,
        primeiro_nome,
        ultimo_nome,
        CONCAT(primeiro_nome, ' ', ultimo_nome) AS nome_colaborador,
        email,
        data_nascimento,
        endereco,
        cep
             
                
    from {{ ref('stg_colaboradores') }}

)

select * from src