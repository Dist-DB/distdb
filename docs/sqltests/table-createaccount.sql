CREATE TABLE `__account` (
  `uid` varchar(34) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '',
  `id_person` varchar(34) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `id_device` varchar(34) DEFAULT NULL,
  `id_organization` varchar(34) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `role` enum('user','admin') CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT 'user',
  `date_created` bigint NOT NULL DEFAULT '0',
  `date_updated` bigint NOT NULL DEFAULT '0',
  `date_lastlogin` bigint NOT NULL DEFAULT '0',
  `is_verified` tinyint unsigned NOT NULL DEFAULT '0',
  `is_deleted` tinyint unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`uid`),
  KEY `id_device` (`id_device`),
  KEY `id_person` (`id_person`),
  CONSTRAINT `__account_ibfk_1` FOREIGN KEY (`id_device`) REFERENCES `__devices` (`uid`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `__account_ibfk_2` FOREIGN KEY (`id_person`) REFERENCES `__person` (`uid`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;