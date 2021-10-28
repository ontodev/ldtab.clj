PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE ldtab(
  'key' TEXT PRIMARY KEY,
  'value' TEXT
);
INSERT INTO ldtab VALUES('ldtab version','0.0.1');
INSERT INTO ldtab VALUES('schema version','0');
CREATE TABLE prefix (
  'prefix' TEXT PRIMARY KEY,
  'base' TEXT NOT NULL
);
CREATE TABLE statement (
  'transaction' INTEGER NOT NULL,
  'retraction' INTEGER NOT NULL DEFAULT 0,
  'graph' TEXT NOT NULL,
  'subject' TEXT NOT NULL,
  'predicate' TEXT NOT NULL,
  'object' TEXT NOT NULL,
  'datatype' TEXT NOT NULL,
  'annotation' TEXT
);
COMMIT;
