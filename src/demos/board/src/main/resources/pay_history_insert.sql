INSERT IGNORE INTO `demo`.`board` (`pay_history`, `create_date`)
VALUES (%f, DATE(CURRENT_TIME()))
     ON DUPLICATE KEY
UPDATE `pay_history` = %f