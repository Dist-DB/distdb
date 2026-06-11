-- distdb parser capability smoke test
-- Covers CREATE DATABASE, USE, CREATE TABLE, SHOW TABLES, SHOW COLUMNS, DESCRIBE.
-- AUTO_INCREMENT is captured as field metadata (no allocator/runtime sequence yet).

CREATE DATABASE analytics;
USE analytics;

CREATE TABLE users (
  id BIGINT NOT NULL PRIMARY KEY,
  email VARCHAR(255),
  age INT,
  is_active TINYINT UNSIGNED NOT NULL
);

SHOW TABLES;
SHOW COLUMNS FROM users;
DESCRIBE users;
