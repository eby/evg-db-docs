.PHONY: generate build clean full

generate:
	uv run python scripts/generate_docs.py

generate-schema:
	uv run python scripts/generate_docs.py --schema $(SCHEMA)

dry-run:
	uv run python scripts/generate_docs.py --dry-run

build:
	./node_modules/.bin/antora antora-playbook.yml

full: generate build

clean:
	rm -rf modules/ROOT/pages/ modules/ROOT/nav.adoc build/
