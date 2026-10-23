MODEL (
  name matrix.full_model,
  kind FULL,
  grain id
);

SELECT
  id,
  name,
  event_date,
  updated_at,
  amount + @VAR('matrix_value', 0) AS amount
FROM matrix.embedded_model;
