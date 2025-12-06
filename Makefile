.PHONY: install project lint build publish package-install

install:
	poetry install

project:
	poetry run project

lint:
	poetry run ruff check .

build:
	poetry build

publish:
	poetry publish --dry-run

package-install:
	python3 -m pip install dist/*.whl
