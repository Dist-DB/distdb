-- distdb dialect compatibility probes
-- Purpose: run statements to verify parser compatibility and current execution wiring.
-- Guidance:
-- 1. Run section A/B statements in order (expected parse + execution success in current stack).
-- 2. Run section C statements individually (expected parser rejection or unsupported path).

-- -----------------------------------------------------------------------------
-- Section A: expected parser + runtime success
-- -----------------------------------------------------------------------------

CREATE DATABASE dialect_probe;
USE dialect_probe;

CREATE TABLE account_probe (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  username VARCHAR(34) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci COMMENT 'login handle',
  role ENUM('user','admin') NOT NULL DEFAULT 'user',
  date_created BIGINT NOT NULL DEFAULT 0,
  is_verified TINYINT UNSIGNED NOT NULL DEFAULT 0
);

SHOW TABLES;
SHOW COLUMNS FROM account_probe;
DESCRIBE account_probe;

CREATE VIEW account_probe_view AS SELECT id, username, role FROM account_probe;
DROP VIEW account_probe_view;

CREATE TRIGGER trg_account_probe_bi
BEFORE INSERT ON account_probe
FOR EACH ROW
BEGIN
END;
DROP TRIGGER IF EXISTS trg_account_probe_bi;

CREATE PROCEDURE p_account_probe()
BEGIN
  SELECT 1;
END;
DROP PROCEDURE p_account_probe;

-- -----------------------------------------------------------------------------
-- Section B: expected parser success but semantic/feature limitations may apply
-- -----------------------------------------------------------------------------

CREATE TABLE account_fk_probe (
  uid VARCHAR(34) NOT NULL,
  id_person VARCHAR(34) DEFAULT NULL,
  role ENUM('user','admin') NOT NULL DEFAULT 'user',
  PRIMARY KEY (uid),
  KEY idx_person (id_person),
  CONSTRAINT account_fk_probe_person_fk FOREIGN KEY (id_person)
    REFERENCES person(uid)
    ON DELETE CASCADE ON UPDATE CASCADE
);

DESCRIBE account_fk_probe;

-- -----------------------------------------------------------------------------
-- Section C: expected unsupported/rejected in current implementation
-- -----------------------------------------------------------------------------

-- Expected parser + runtime success for current scalar UDF lifecycle support.
CREATE FUNCTION f_probe() RETURNS INT RETURN 1;
DROP FUNCTION f_probe;

-- Expected unsupported in current classifier wiring.
EXPLAIN SELECT * FROM account_probe;
