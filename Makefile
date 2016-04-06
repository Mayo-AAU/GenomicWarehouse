define help

Genomics Warehouse

documentation:
  install   - build apps and install on test cluster (use SSH )
  setup   - install gitbook, assumes Node / npm are installed
  build   - build the documentation, in documentation/static
  watch   - watch for documentation changes, serves on http://localhost:4000
  open    - Open the documentation website (http://localhost:4000)

endef
export help

help:
	@echo "$$help"



# Set the SSH target if not already set
install:
	# (cd gwarehouse && mvn package)
	rsync -r devops/ darkhorse:DJB/
	rsync -r --delete warehouse/target/lib darkhorse:DJB/
	rsync -r warehouse/target/warehouse-1.0-SNAPSHOT.jar darkhorse:DJB/



# Documentation
setup: node_modules/gitbook-cli
# installs gitbook
node_modules/gitbook-cli:
	@[ -x "`which npm 2>/dev/null`" ] || (printf "\n=====\nCould not find npm in your PATH, please install from http://nodejs.org\n=====\n\n"; exit 1;)
	npm install gitbook-cli

build: setup
	node_modules/.bin/gitbook build

watch: setup
	node_modules/.bin/gitbook serve

open:
	open http://localhost:4000

clean:
	rm -rf doc/_book

.PHONY: help setup build npm install
