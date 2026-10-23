MODEL (
  name matrix.view_model,
  kind VIEW,
  grain id
);

SELECT
  id,
  name,
  amount
FROM matrix.full_model;
