{{ config(materialized='table') }}

with base as (
    select
        toDate(transaction_time) as transaction_date,
        us_state,
        amount,
        amount_bucket
    from {{ ref('stg_transactions') }}
)

select
    transaction_date,
    us_state,
    count() as transactions_cnt,
    sum(amount) as transactions_sum,
    avg(amount) as avg_amount,
    quantileExact(0.95)(amount) as p95_amount,
    sumIf(1, amount_bucket = 'high') / count() as share_high_transactions
from base
group by
    transaction_date,
    us_state
order by
    transaction_date,
    us_state
