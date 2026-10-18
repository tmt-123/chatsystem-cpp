USE `bite_im`;

-- Apply once after 002; preserves all existing message rows.
ALTER TABLE `message`
  DROP INDEX `client_message_id_i`,
  ADD UNIQUE INDEX `sender_client_message_id_i` (`user_id`, `client_message_id`);
