{{
    config(
        materialized = 'view'
    )
}}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone_number,
    age::INTEGER,
    country,
    city,
    operator,
    plan_type,
    monthly_data_gb::NUMERIC,
    monthly_bill_usd::NUMERIC,
    CASE
        WHEN registration_date ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(registration_date, 'DD/MM/YYYY')
        WHEN registration_date ~ '^\d{4}-\d{2}-\d{2}$' THEN to_date(registration_date, 'YYYY-MM-DD')
        ELSE NULL
    END AS registration_date,
    status,
    device_brand,
    device_model,
    contracted_services,
    payment_history,
    credit_limit::NUMERIC,
    data_usage_current_month::NUMERIC,
    latitude::NUMERIC,
    longitude::NUMERIC,
    credit_score::NUMERIC,
    ingestion_date,
    source
FROM 
    bronze.raw
