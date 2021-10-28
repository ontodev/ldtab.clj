# LDTab: Linked Data Tables

LDTab is a tool for working with RDF Datasets and OWL using SQL databases.
The immediate use case for LDTab is an ontology term browser
with support for history and multiple named graphs.
The current version is focused on embedded database use case, building on SQLite.

## Init

First initialize the SQLite file with required tables:

```sh file=tests/init/test.sh
ldtab init test.db
```

This creates tables with the following structure:

- ldtab: metadata
    - key
    - value
- prefix: used to convert between IRIs and CURIEs
    - prefix: the short prefix string
    - base: its expansion
- statement
    - transaction: an integer indicating when the statement was asserted
    - retraction: an integer indicating when the statement was retracted;
      defaults to 0, which means **no** retraction;
      must be greater than the transaction
    - graph
    - subject
    - predicate
    - object
    - datatype
    - annotation: for RDF reifications and OWL annotations

## Prefix

Prefixes are used to shorten IRIs to CURIEs.
Add your preferred prefixes from a TSV file:

```sh file=tests/prefix/test.sh
ldtab prefix test.db prefix.tsv
```

## Import

You can now import an RDFXML file into the 'statement' table
using those prefixes:

```sh file=tests/import-v1/test.sh
ldtab import test.db test-v1.owl
```

The first import will result in transaction 1.
The next import will result in the next transaction, and so on.

## Export

You can export to a file

```sh file=tests/export-v2/test.sh
ldtab export test.db test-v2.ttl
```

