with src as (
    select

    p.cod_proposta,
    p.cod_cliente,
    p.cod_colaborador,
    ca.cod_agencia as cod_agencia_colaborador,
    p.data_entrada_proposta,
    p.taxa_juros_mensal,
    p.valor_proposta,
    p.valor_financiamento,
    p.valor_entrada,
    p.valor_prestacao,
    p.quantidade_parcelas,
    p.carencia,
    p.status_proposta

    from {{ ref('stg_propostas_credito')}} as p
    left join
        {{ ref('stg_colaborador_agencia')}} as ca
        on
        p.cod_colaborador = ca.cod_colaborador
    
)

select * from src