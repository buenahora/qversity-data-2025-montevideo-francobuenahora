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