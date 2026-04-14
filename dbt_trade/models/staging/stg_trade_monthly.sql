with src as (
    select * from {{ source('trade_raw', 'fct_trade_monthly') }}
)
select
    trade_month,
    reporter_iso,
    partner_iso,
    hs_chapter,
    trade_flow,
    trade_value_usd
from src
where trade_value_usd is not null
  and partner_iso is not null
  and hs_chapter is not null
