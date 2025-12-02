{{ config(
    materialized='table',
    alias='mart_fraud_by_state'
) }}

with base as (
    select
        us_state,
        count() as total_transactions,
        sum(is_fraud) as fraud_transactions,
        sum(amount) as total_amount,
        sumIf(amount, is_fraud = 1) as fraud_amount,
        countDistinct(concat(name_1, ' ', name_2)) as unique_customers,
        countDistinct(merch) as unique_merchants
    from {{ ref('stg_transactions') }}
    group by us_state
)

select
    us_state,
    total_transactions,
    fraud_transactions,
    (fraud_transactions / total_transactions) * 100 as fraud_rate,
    unique_customers,
    unique_merchants,
    total_amount,
    fraud_amount
from base
order by fraud_rate desc;
