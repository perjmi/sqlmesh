AUDIT (
  name randomgraph_invariants,
  blocking true
);

SELECT *
FROM @this_model
WHERE
  entity_id IS NULL
  OR bucket_id IS NULL
  OR total_value IS NULL
  OR row_count IS NULL
  OR row_count <= 0;
