.PHONY: dev
dev:
	python3.11 -m venv .venv-infra --upgrade-deps
	.venv-infra/bin/pip3 install -r requirements-infra.txt
	.venv-infra/bin/pre-commit install
	.venv-infra/bin/poetry self add poetry-bumpversion
	.venv-infra/bin/poetry install --no-root

.PHONY: lint
lint:
	flake8 .

.PHONY: format
format:
	black --check .

.PHONY: format-fix
format-fix:
	isort .
	black .

.PHONY: proto
proto:
	./proto/generate.sh

.PHONY: fix-all
fix-all:
	pre-commit run --all-files

.PHONY: mypy
mypy:
	mypy --show-error-codes etcd3

.PHONY: test
test:
	poetry run pytest -cov

.PHONY: build
build:
	poetry build
