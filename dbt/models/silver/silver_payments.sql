{{ config(materialized = 'table') }}

-- 1️⃣ Explota el array de pagos sin WITH ORDINALITY
WITH exploded AS (
    SELECT
        s.customer_id,
        jsonb_array_elements(s.payment_history::jsonb) AS p
    FROM {{ ref('staging_mobile_raw') }} s
    WHERE s.payment_history IS NOT NULL
      AND jsonb_typeof(s.payment_history::jsonb) = 'array'
),

-- 2️⃣ Filtra clientes válidos
valid_payments AS (
    SELECT e.*
    FROM exploded e
    JOIN {{ ref('silver_customers') }} c
      ON e.customer_id = c.customer_id
),

-- 3️⃣ Elimina duplicados perfectos con ROW_NUMBER y filtra la primera ocurrencia
deduplicated AS (
    SELECT *
    FROM (
        SELECT
            customer_id,
            p ->> 'date'   AS raw_date,
            p ->> 'amount' AS raw_amount,
            p ->> 'status' AS raw_status,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id, p ->> 'date', p ->> 'amount', p ->> 'status'
                ORDER BY p ->> 'date'
            ) AS rn
        FROM valid_payments
    ) sub
    WHERE rn = 1
)

-- 4️⃣ Salida final con ID estable y seguro
SELECT
    md5(
        concat_ws(
            '|',
            customer_id,
            raw_date,
            COALESCE(raw_amount, 'NULL'),
            raw_status
        )
    ) AS payment_id,

    customer_id,

    NULLIF(raw_date::DATE, NULL) AS payment_date,

    CASE
        WHEN raw_amount ~ '^[0-9]+(\.[0-9]+)?$'
             THEN GREATEST(raw_amount::NUMERIC, 0)
        ELSE 0
    END AS payment_amount,

    INITCAP(raw_status) AS payment_status,

    CURRENT_TIMESTAMP AS run_ts

FROM deduplicated
