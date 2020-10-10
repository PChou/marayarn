
DROP TABLE IF EXISTS `tb_application`;
CREATE TABLE `tb_application`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `group_id` bigint(20) NOT NULL DEFAULT -1,
  `absolute_path` varchar(255) NOT NULL,
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
  INDEX `idx_app_gid`(`group_id`) USING BTREE,
  INDEX `idx_abs_path`(`absolute_path`(50)) USING BTREE
);


DROP TABLE IF EXISTS `tb_artifact`;
CREATE TABLE `tb_artifact`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `hdfs_addr` varchar(50) NULL,
  `directory` varchar(100) NOT NULL,
  `create_time` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
);


DROP TABLE IF EXISTS `tb_group`;
CREATE TABLE `tb_group`  (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(50) NOT NULL,
  `parentId` bigint(20) NOT NULL DEFAULT -1,
  `absolute_path` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
);


