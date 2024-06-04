### Configuration
#
# These are standard options to make Make sane:
# <http://clarkgrubb.com/makefile-style-guide#toc2>

MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help
.DELETE_ON_ERROR:
.PRECIOUS:
.SUFFIXES:

export PATH := $(shell pwd)/bin:$(PATH)

DB := build/database.db
ontology := resources/ontology.owl
LEIN := lein

### Main Tasks

.PHONY: help
help:
	@echo "LDTab Makefile"
	@echo ""
	@echo "TASKS"
	@echo "  ldtab       build LDTab executable"
	@echo "  test        run round-trip tests"
	@echo "  all         build all files"
	@echo "  clean       remove all build files"
	@echo "  clobber     remove all generated files"
	@echo "  help        print this message"

.PHONY: ldtab
ldtab: bin/ldtab

.PHONY: all
all: ldtab bin/robot test

.PHONY: clean
clean:
	rm -rf build/

.PHONY: clobber
clobber:
	rm -rf bin/ build/

bin/ build/:
	mkdir -p $@

### Install Dependencies

# Require SQLite
ifeq ($(shell command -v sqlite3),)
$(error 'Please install SQLite 3')
endif

# Require Java
ifeq ($(shell command -v java),)
$(error 'Please install Java, so we can run ROBOT and LDTab')
endif

# Require Leiningen
ifeq ($(shell command -v lein),)
$(error 'Please install Leiningen, so we can build LDTab')
endif

# Install ROBOT
bin/robot.jar: | bin/
	curl -L -o $@ 'https://github.com/ontodev/robot/releases/download/v1.9.5/robot.jar'

bin/robot: bin/robot.jar
	curl -L -o $@ 'https://raw.githubusercontent.com/ontodev/robot/master/bin/robot'
	chmod +x $@

# Install LDTab 
bin/ldtab.jar: | bin/
	$(LEIN) uberjar
	mv target/uberjar/ldtab-0.1.0-SNAPSHOT-standalone.jar $@

bin/ldtab: bin/ldtab.jar
	echo '#!/bin/sh' > $@
	echo 'java -jar "$$(dirname $$0)/ldtab.jar" "$$@"' >> $@
	chmod +x $@

### Round-trip Test
# The test ontology was taken from here: https://raw.githubusercontent.com/ontodev/robot/master/robot-core/src/test/resources/axioms.owl

.PHONY: test
test: tests/prefix/prefix.tsv | bin/ldtab bin/robot build/ $(ontology)
	echo "Testing round-trip"
	rm -f $(DB)
	rm -f build/ontology.ttl
	rm -f build/ontology.owl
	ldtab init $(DB) 
	ldtab prefix $(DB) $<
	ldtab import $(DB) $(ontology) 
	ldtab export $(DB) build/ontology.ttl --format ttl
	robot convert --input build/ontology.ttl --output build/ontology.owl
	robot diff --left $(ontology) --right build/ontology.owl \
	| grep 'Ontologies are identical'
