.PHONY: dev
dev:
	python3.11 -m venv .venv --upgrade-deps
	.venv/bin/pip3 install -r requirements-dev.txt
	.venv/bin/pip3 install -r requirements.txt
	.venv/bin/pre-commit install

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

.PHONY: mypy
test:
	pytest -cov
