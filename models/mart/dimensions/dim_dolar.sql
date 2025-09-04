with src as (
    select
        d.date_day,
        s.cotacao_dolar
    from {{ ref('dim_dates') }} as d
    left join {{ ref('stg_bacen_dolar') }} as s
        on d.date_day = s.date_day
)

select *
from src
