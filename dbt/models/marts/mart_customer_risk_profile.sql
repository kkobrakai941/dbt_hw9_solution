{{ config(
    materialized='table',
    alias='mart_customer_risk_profile'
) }}

with base as (
    select
        concat(name_1, ' ', name_2) as customer_name,
        count() as transaction_count,
        sum(amount) as total_amount,
        avg(amount) as avg_amount,
        sum(is_fraud) as fraud_transactions
    from {{ ref('stg_transactions') }}
    group by customer_name
)

select
    customer_name,
    transaction_count,
    total_amount,
    avg_amount,
    fraud_transactions,
    (fraud_transactions / transaction_count) as fraud_rate,
    case
        when (fraud_transactions / transaction_count) > 0.05 then 'HIGH'
        when (fraud_transactions / transaction_count) > 0.01 then 'MEDIUM'
        else 'LOW'
    end as risk_level
from base
order by fraud_rate desc;
