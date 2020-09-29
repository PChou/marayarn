
DROP TABLE IF EXISTS `tb_application`;
CREATE TABLE `tb_application`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `group_id` bigint(20) NOT NULL DEFAULT -1,
  `name` varchar(50) NOT NULL,
  `yarn_application_id` varchar(50) NULL,
  `status` varchar(20) NOT NULL,
  `command` varchar(1024) NULL,
  `cpu` tinyint(8) NOT NULL,
  `memory` int(11) NOT NULL,
  `instance_count` int(11) NULL,
  `constraints` varchar(1024) NULL,
  `queue` varchar(255) NULL,
  `user` varchar(255) NULL,
  `version_time` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `idx_app_gid`(`group_id`) USING BTREE
);


