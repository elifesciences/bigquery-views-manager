SELECT
  view1.*,
  CHAR_LENGTH(value) AS value_length
FROM `{project}.{dataset}.mv_view1` AS view1
