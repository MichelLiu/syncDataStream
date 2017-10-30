/*
Navicat MySQL Data Transfer

Source Server         : 127.0.0.1_3306
Source Server Version : 50634
Source Host           : 127.0.0.1:3306
Source Database       : datastream

Target Server Type    : MYSQL
Target Server Version : 50634
File Encoding         : 65001

Date: 2017-10-30 13:33:09
*/

CREATE DATABASE IF NOT EXISTS datastream default charset utf8 COLLATE utf8_general_ci;
USE datastream;

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for data_stream_config
-- ----------------------------
CREATE TABLE IF NOT EXISTS `data_stream_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `source` text CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `target` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `occurs` int(11) NOT NULL,
  `upsert` int(11) NOT NULL DEFAULT '0',
  `fieldMap` text CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `update_time` int(11) NOT NULL,
  `author` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT '',
  `status` int(11) NOT NULL DEFAULT '0',
  `description` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT '',
  `pyscript` text CHARACTER SET utf8 COLLATE utf8_bin,
  `ip` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT '',
  `pid` int(11) DEFAULT '-1',
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`) USING BTREE,
  UNIQUE KEY `name` (`name`) USING BTREE,
  KEY `status` (`status`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for data_stream_progress
-- ----------------------------
CREATE TABLE IF NOT EXISTS `data_stream_progress` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `threadid` varchar(128) NOT NULL,
  `total` bigint(20) NOT NULL DEFAULT '0',
  `status` int(11) NOT NULL DEFAULT '0',
  `position` varchar(128) DEFAULT NULL,
  `create_time` int(11) NOT NULL,
  `end_time` int(11) DEFAULT NULL,
  `finished` bigint(20) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`,`threadid`) USING BTREE,
  KEY `query` (`id`,`threadid`,`status`) USING BTREE,
  KEY `threadid` (`threadid`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;
