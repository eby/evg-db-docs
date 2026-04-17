.PHONY: generate generate-schema dry-run changelog build full clean

generate:
	uv run python scripts/generate_docs.py

generate-schema:
	uv run python scripts/generate_docs.py --schema $(SCHEMA)

dry-run:
	uv run python scripts/generate_docs.py --dry-run

changelog:
	uv run python scripts/generate_changelog.py

build:
	./node_modules/.bin/antora antora-playbook.yml

full: generate changelog build

clean:
	rm -rf modules/ROOT/pages/ modules/ROOT/nav.adoc build/
