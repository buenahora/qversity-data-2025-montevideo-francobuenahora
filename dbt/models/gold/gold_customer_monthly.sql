{{ config(materialized='table') }}

WITH customers AS (
  SELECT
    customer_id,
    registration_date,
    device_id,
    status,
    credit_score
  FROM 
    {{ ref('silver_customers') }}
  WHERE 
    registration_date <= CURRENT_DATE
),

months AS (
  SELECT date_month
  FROM 
    {{ ref('gold_date') }}
  WHERE 
    date_month <= CURRENT_DATE
),

monthly_payments AS (
  SELECT
    customer_id,
    date_month,
    total_paid_usd,
    num_payments,
    latest_status
  FROM 
    {{ ref('gold_payment') }}
),

plans AS (
  SELECT 
    plan_id, 
    operator_id, 
    monthly_bill_usd
  FROM 
    {{ ref('silver_plans') }}
),

customer_plan AS (
  SELECT DISTINCT ON (customer_id)
    customer_id,
    plan_id
  FROM 
    {{ ref('silver_customers_plans') }}
),

customer_months AS (
  SELECT
    c.customer_id,
    d.date_month,
    cp.plan_id,
    p.operator_id,
    c.device_id,
    c.status,
    c.credit_score,

    CASE 
      WHEN d.date_month = DATE_TRUNC('month', c.registration_date)
      THEN TRUE 
      ELSE FALSE 
    END AS is_new_customer,

    p.monthly_bill_usd,
    pay.total_paid_usd,
    pay.latest_status,
    pay.num_payments,

    CASE 
      WHEN pay.latest_status IS NULL THEN FALSE
      WHEN pay.latest_status NOT IN ('Paid') THEN TRUE
      ELSE FALSE
    END AS has_payment_issue

  FROM 
  customers c
  CROSS JOIN 
    months d
  LEFT JOIN 
    customer_plan cp ON c.customer_id = cp.customer_id
  LEFT JOIN 
    plans p ON cp.plan_id = p.plan_id
  LEFT JOIN 
    monthly_payments pay ON c.customer_id = pay.customer_id AND d.date_month = pay.date_month
  WHERE 
    d.date_month >= DATE_TRUNC('month', c.registration_date)

)

SELECT 
  customer_id,
  date_month,
  plan_id,
  operator_id,
  is_new_customer,
  status,
  credit_score,
  device_id,
  monthly_bill_usd,
  total_paid_usd,
  latest_status,
  num_payments,
  has_payment_issue,
  CURRENT_TIMESTAMP AS run_ts

FROM 
  customer_months
ORDER BY 
  customer_id, 
  date_month
