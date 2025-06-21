
-- Verifica que la tabla silver_devices cumple con las reglas de calidad de datos
-- Reglas de calidad:
-- 1. Silver devices debe tener un modelo y marca de dispositivo únicos

SELECT d.*
FROM 
    {{ ref('silver_devices') }} AS d
INNER JOIN (

    SELECT device_brand,
           device_model
    FROM 
        {{ ref('silver_devices') }}
    GROUP BY 
        device_brand, 
        device_model
    HAVING COUNT(*) > 1

) AS dups
    ON d.device_brand = dups.device_brand
    AND d.device_model = dups.device_model