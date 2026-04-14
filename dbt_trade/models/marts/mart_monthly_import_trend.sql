-- Dashboard tile: temporal trend (total reported imports by month)
{{ config(
    alias='mart_monthly_import_trend',
    partition_by={'field': 'trade_month', 'data_type': 'date'},
    cluster_by=['reporter_iso'],
) }}

select
    trade_month,
    reporter_iso,
    round(sum(trade_value_usd), 2) as import_value_usd
from {{ ref('stg_trade_monthly') }}
where trade_flow = 'Import'
group by trade_month, reporter_iso
order by trade_month
