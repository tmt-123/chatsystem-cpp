USE `bite_im`;
CREATE TABLE IF NOT EXISTS `user_search_outbox` (
  `id` BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `user_id` varchar(64) NOT NULL,
  KEY `user_id_i` (`user_id`)
) ENGINE=InnoDB;

-- Backfill also repairs accounts left unindexed by earlier partial failures.
INSERT INTO `user_search_outbox` (`user_id`)
SELECT `user_id` FROM `user`;
