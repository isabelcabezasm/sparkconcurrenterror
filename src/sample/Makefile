.PHONY: clean clean-test clean-pyc clean-build help
.DEFAULT_GOAL := help

PYTHON_PACKAGE := sample

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	rm -fr descriptor_build/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

lint: ## static code analsysis
	mypy -p $(PYTHON_PACKAGE) -p tests
	flake8

dist: clean ## builds source and wheel package
	python -m build -w

install: clean ## install the package to the active Python's site-packages
	pip install .

install-dev: clean ## installs the package, including all optional dependencies, to the active Python's site-packages
	pip install .[test] .[descriptor] .[develop] .[build]
