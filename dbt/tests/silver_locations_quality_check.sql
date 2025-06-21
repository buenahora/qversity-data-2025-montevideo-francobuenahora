
-- Verifica que los datos en silver_locations cumplen con las reglas de calidad
-- Reglas de calidad:
-- 1. Latitud debe estar entre -90 y 90
-- 2. Longitud debe estar entre -180 y 180
-- 3. Latitud y longitud no deben ser nulos


SELECT *
FROM 
   {{ ref('silver_locations') }}
WHERE latitude IS NULL
   OR longitude IS NULL
   OR latitude NOT BETWEEN -90 AND 90
   OR longitude NOT BETWEEN -180 AND 180