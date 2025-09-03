with src as (

    select
        co.cod_colaborador,
        ca.cod_agencia,
        co.primeiro_nome,
        co.ultimo_nome,
        co.email,
        co.cpf,
        co.data_nascimento,
        co.endereco,
        co.cep
             
                
    from {{ ref('stg_colaboradores') }} AS co
    left join 
        {{ ref('stg_colaborador_agencia')}} AS ca
        on
        co.cod_colaborador = ca.cod_colaborador    

)

select * from src