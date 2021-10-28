sqlite3 test.db .dump > actual.sql
diff expected.sql actual.sql
