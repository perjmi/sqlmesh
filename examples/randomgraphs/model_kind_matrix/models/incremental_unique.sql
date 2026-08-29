MODEL (
  name matrix.incremental_unique_model,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key id
  ),
  cron '@daily',
  grain id
);

SELECT
  id,
  name,
  event_date,
  amount + @VAR('matrix_value', 0) AS amount
FROM matrix.embedded_model;
