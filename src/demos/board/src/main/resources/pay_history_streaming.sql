SELECT
  (SUM(get_json_object(`data`, '$.pay_amount'))  + %f) AS pay_history
FROM
  source
WHERE
  `table` = 'orders' and `type` = 'insert'
  and ISNOTNULL(get_json_object(`data`, '$.pay_amount'))
  and date(get_json_object(`data`, '$.create_time')) >= date('%s')