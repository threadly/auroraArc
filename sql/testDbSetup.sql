CREATE USER 'auroraArc'@'%' IDENTIFIED BY '';

CREATE DATABASE auroraArc;

GRANT ALL PRIVILEGES ON auroraArc.* TO 'auroraArc'@'%';

mysql --user=auroraArc auroraArc

CREATE TABLE `records` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `created_date` datetime NOT NULL,
  `value` varchar(64) NOT NULL,
  PRIMARY KEY(`id`),
  KEY `ix_entry_event_date` (`created_date`)
) ENGINE=InnoDB DEFAULT CHARSET=ascii DEFAULT COLLATE=ascii_bin;
