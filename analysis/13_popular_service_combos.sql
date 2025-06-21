-- 13_popular_service_combos.sql
WITH combos AS (
    SELECT
        customer_id,
        STRING_AGG(DISTINCT service_name, ' + ' ORDER BY service_name) AS combo
    FROM
        gold.gold_service_client_monthly
    WHERE
        is_active
    GROUP BY
        customer_id
)
SELECT
    combo,
    COUNT(*) AS combo_freq
FROM combos
GROUP BY combo
ORDER BY combo_freq DESC
LIMIT 20;
