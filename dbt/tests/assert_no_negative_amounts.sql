-- tests/assert_no_negative_amounts.sql
SELECT *
FROM {{ ref('stg_transactions') }}WHERE amount < 0;
