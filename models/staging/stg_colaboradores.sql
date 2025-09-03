with src as (

    select
        cod_colaborador,
        primeiro_nome,
        ultimo_nome,
        email,
        regexp_replace(cpf, r'[^0-9]', '') as cpf,
        data_nascimento,
        endereco,
        regexp_replace(cep, r'[^0-9]', '') as cep     
                
    from {{ source('raw', 'colaboradores') }}

)


select * from src