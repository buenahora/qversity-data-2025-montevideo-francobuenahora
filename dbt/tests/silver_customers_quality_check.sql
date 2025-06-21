-- Verifica que la tabla silver_customers cumple con las reglas de calidad de datos
-- Reglas de calidad:
-- 1. Edad debe estar entre 0 y 119 años
-- 2. Puntaje de crédito debe estar entre 300 y 850
-- 3. Estado debe ser 'Active', 'Inactive' o 'Suspended'
-- 4. Correo electrónico debe tener un formato válido

SELECT *
FROM 
  {{ ref('silver_customers') }}
WHERE 
  age NOT BETWEEN 0 AND 119
  OR credit_score NOT BETWEEN 300 AND 850
  OR INITCAP(status) NOT IN ('Active','Inactive','Suspended')
  OR email !~* '^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$'