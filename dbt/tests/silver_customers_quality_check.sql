SELECT *
FROM 
  {{ ref('silver_customers') }}
WHERE 
  age NOT BETWEEN 0 AND 119
  OR credit_score NOT BETWEEN 300 AND 850
  OR INITCAP(status) NOT IN ('Active','Inactive','Suspended')
  OR email !~* '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'