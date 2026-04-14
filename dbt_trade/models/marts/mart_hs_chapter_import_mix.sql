-- Dashboard tile: categorical distribution (HS2 chapter share of imports)
{{ config(
    alias='mart_hs_chapter_import_mix',
    cluster_by=['hs_chapter'],
) }}

with agg as (
    select
        hs_chapter,
        round(sum(trade_value_usd), 2) as import_value_usd
    from {{ ref('stg_trade_monthly') }}
    where trade_flow = 'Import'
    group by hs_chapter
),
tot as (
    select sum(import_value_usd) as grand_total from agg
)
select
    agg.hs_chapter,
    agg.import_value_usd,
    round(100.0 * agg.import_value_usd / nullif(tot.grand_total, 0), 2) as pct_of_total
from agg
cross join tot
order by agg.import_value_usd desc
