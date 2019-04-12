INSERT IGNORE INTO `demo`.`board` (`pay_today`, `create_date`)
VALUES (%f , DATE(CURRENT_TIME()))
     ON DUPLICATE KEY
UPDATE `pay_today` = %f