MODEL (
  name matrix.scd_time_model,
  kind SCD_TYPE_2_BY_TIME (
    unique_key id,
    updated_at_name updated_at,
    valid_from_name valid_from,
    valid_to_name valid_to
  ),
  cron '@daily',
  grain [id, valid_from]
);

SELECT
  id,
  name,
  updated_at,
  amount + @VAR('matrix_value', 0) AS amount
FROM matrix.embedded_model;
