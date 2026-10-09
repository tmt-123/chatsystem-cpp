USE `bite_im`;

-- 为历史消息回填客户端幂等 ID；新消息使用客户端生成的 UUID。
ALTER TABLE `message`
  ADD COLUMN `client_message_id` varchar(64) NULL AFTER `message_id`;

UPDATE `message`
SET `client_message_id` = `message_id`
WHERE `client_message_id` IS NULL;

ALTER TABLE `message`
  MODIFY COLUMN `client_message_id` varchar(64) NOT NULL,
  ADD UNIQUE INDEX `client_message_id_i` (`client_message_id`);
