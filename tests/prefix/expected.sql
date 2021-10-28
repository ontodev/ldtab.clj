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
INSERT INTO prefix VALUES('rdf','http://www.w3.org/1999/02/22-rdf-syntax-ns#');
INSERT INTO prefix VALUES('rdfs','http://www.w3.org/2000/01/rdf-schema#');
INSERT INTO prefix VALUES('xsd','http://www.w3.org/2001/XMLSchema#');
INSERT INTO prefix VALUES('owl','http://www.w3.org/2002/07/owl#');
INSERT INTO prefix VALUES('ex','http://example.com');
INSERT INTO prefix VALUES('ldtab','https://ldtab.org/2021/10/ldtab#');
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
