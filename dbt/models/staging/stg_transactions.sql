{{ config(materialized='table') }}

with source as (
    select * from {{ source('transactions_db', 'transactions') }}
)

select
    toDateTime(transaction_time) as transaction_time,
    merch,
    cat_id,
    toDecimal64(amount, 2) as amount,
    name_1,
    name_2,
    gender,
    street,
    one_city,
    us_state,
    toUInt32(post_code) as post_code,
    lat,
    lon,
    population_city,
    jobs,
    merchant_lat,
    merchant_lon,
    target as is_fraud,
    {{ amount_bucket(amount) }} as amount_bucket
from source
