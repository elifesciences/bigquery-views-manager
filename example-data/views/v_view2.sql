SELECT
  view1.*,
  CHAR_LENGTH(value1) AS value1_length
FROM `{project}.{dataset}.mv_view1` AS view1
