{{ config(materialized = 'table') }}

WITH exploded AS (
    SELECT
        s.customer_id,
        jsonb_array_elements(s.payment_history::jsonb) AS p
    FROM 
        {{ ref('staging_mobile_raw') }} s
    WHERE 
        s.payment_history IS NOT NULL
        AND jsonb_typeof(s.payment_history::jsonb) = 'array'
),

-- valid customers only
valid_payments AS (
    SELECT
        e.*
    FROM exploded e
    JOIN {{ ref('silver_customers') }} c ON e.customer_id = c.customer_id
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

    (p ->> 'date')::DATE AS payment_date,

    CASE
        WHEN (p ->> 'amount') ~ '^[0-9]+(\.[0-9]+)?$'
            THEN greatest((p ->> 'amount')::NUMERIC, 0)
        ELSE 0
    END AS payment_amount,
    
    initcap(p ->> 'status') AS payment_status,
    CURRENT_TIMESTAMP AS run_ts

FROM valid_payments
