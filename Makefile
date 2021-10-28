help:
	@echo "USAGE:"
	@echo "    make update    update tests/ from README.md"

build:
	mkdir -p $@

build/waltz: | build
	curl -L -o build/waltz.tar.gz https://github.com/killercup/waltz/releases/download/waltz_cli-0.1.4/waltz-waltz_cli-0.1.4-x86_64-unknown-linux-musl.tar.gz
	cd build && tar xvf waltz.tar.gz

.PHONY: update
update: build/waltz
	build/waltz README.md --target_dir .

