{{ config(
    materialized='table',
    alias='mart_fraud_by_category'
) }}

with base as (
    select
        cat_id,
        count() as total_transactions,
        sum(is_fraud) as fraud_transactions,
        sum(amount) as total_amount,
        sumIf(amount, is_fraud = 1) as fraud_amount
    from {{ ref('stg_transactions') }}
    group by cat_id
)

select
    cat_id,
    total_transactions,
    fraud_transactions,
    (fraud_transactions / total_transactions) * 100 as fraud_rate,
    total_amount,
    fraud_amount
from base
order by fraud_rate desc;
