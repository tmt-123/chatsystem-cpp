USE `bite_im`;

-- PBKDF2 编码包含算法、迭代次数、盐值和摘要，需要扩大密码列。
ALTER TABLE `user`
  MODIFY COLUMN `password` varchar(255) NULL;
