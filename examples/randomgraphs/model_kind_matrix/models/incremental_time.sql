MODEL (
  name matrix.incremental_time_model,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date
  ),
  cron '@daily',
  grain [id, event_date]
);

SELECT
  id,
  name,
  event_date,
  amount + @VAR('matrix_value', 0) AS amount
FROM matrix.embedded_model
WHERE event_date BETWEEN @start_date AND @end_date;
