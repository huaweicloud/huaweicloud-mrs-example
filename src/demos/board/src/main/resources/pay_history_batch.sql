SELECT
  CAST (SUM(`pay_amount`) AS DOUBLE)
FROM
  orders
WHERE
  date(`create_time`) < date(now())