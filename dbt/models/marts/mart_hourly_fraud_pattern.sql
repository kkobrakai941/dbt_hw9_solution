{{ config(
    materialized='table',
    alias='mart_hourly_fraud_pattern'
) }}

with base as (
    select
        toDayOfWeek(transaction_time) as day_of_week,
        toHour(transaction_time) as hour,
        count() as total_transactions,
        sum(is_fraud) as fraud_transactions,
        sum(amount) as total_amount,
        avg(amount) as avg_amount
    from {{ ref('stg_transactions') }}
    group by day_of_week, hour
)

select
    day_of_week,
    hour,
    total_transactions,
    fraud_transactions,
    (fraud_transactions / total_transactions) * 100 as fraud_rate,
    total_amount,
    avg_amount
from base
order by fraud_rate desc;
