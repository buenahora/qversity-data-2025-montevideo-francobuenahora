select *
from {{ ref('silver_customers') }}
where
  age < 0
  or age >= 120
  or credit_score < 300
  or credit_score > 850
  or INITCAP(status) not in ('Active','Inactive','Suspended')