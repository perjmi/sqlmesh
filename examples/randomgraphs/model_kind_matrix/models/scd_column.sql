MODEL (
  name matrix.scd_column_model,
  kind SCD_TYPE_2_BY_COLUMN (
    unique_key id,
    columns [name, amount],
    valid_from_name valid_from,
    valid_to_name valid_to,
    execution_time_as_valid_from true
  ),
  cron '@daily',
  grain [id, valid_from]
);

SELECT
  id,
  name,
  amount + @VAR('matrix_value', 0) AS amount
FROM matrix.embedded_model;
