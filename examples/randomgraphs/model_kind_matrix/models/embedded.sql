MODEL (
  name matrix.embedded_model,
  kind EMBEDDED
);

SELECT
  id,
  name,
  event_date,
  updated_at,
  amount
FROM matrix.seed_model;
