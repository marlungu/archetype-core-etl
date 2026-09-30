-- Audit evidence queries for the governed reference system.
SELECT
  event_time,
  user_identity.email AS actor,
  service_name,
  action_name,
  request_params,
  response.status_code AS status_code
FROM system.access.audit
WHERE event_date >= current_date() - INTERVAL 7 DAYS
  AND (
    request_params.catalog_name = 'archetype_core'
    OR request_params.full_name LIKE 'archetype_core.governed.%'
  )
ORDER BY event_time DESC
LIMIT 500;

SHOW GRANTS ON SCHEMA archetype_core.governed;
