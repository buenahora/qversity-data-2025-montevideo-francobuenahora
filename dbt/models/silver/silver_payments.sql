{{ config(materialized = 'table') }}

WITH exploded AS (

    SELECT
        customer_id,
        jsonb_array_elements(payment_history::jsonb) AS p
    FROM 
        {{ ref('staging_mobile_raw') }}
    WHERE 
        payment_history IS NOT NULL
        AND jsonb_typeof(payment_history::jsonb) = 'array'

)

SELECT
    md5(
        concat_ws(
            '|',
            customer_id,
            p ->> 'date',
            COALESCE(p ->> 'amount', 'NULL'),
            p ->> 'status'
        )
    ) AS payment_id,

    customer_id,
    (p ->> 'date')::date AS payment_date,

    CASE
        WHEN (p ->> 'amount') ~ '^[0-9]+(\.[0-9]+)?$'
            THEN greatest((p ->> 'amount')::numeric, 0)
        ELSE 0
    END AS payment_amount,
    p ->> 'status' AS payment_status

FROM 
    exploded
WHERE 
    customer_id IS NOT NULL
