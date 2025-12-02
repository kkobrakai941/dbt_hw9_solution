{{ config(
    materialized='table',
    alias='mart_merchant_analytics'
) }}

with base as (
    select
        merch,
        count() as total_transactions,
        sum(amount) as total_amount,
        avg(amount) as avg_amount,
        sum(is_fraud) as fraud_transactions
    from {{ ref('stg_transactions') }}
    group by merch
)

select
    merch,
    total_transactions,
    total_amount,
    avg_amount,
    fraud_transactions,
    (fraud_transactions / total_transactions) * 100 as fraud_rate,
    case
        when (fraud_transactions / total_transactions) > 0.02 then 1
        else 0
    end as is_suspicious
from base
order by fraud_rate desc;
