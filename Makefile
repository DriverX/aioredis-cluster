PYTHON ?= python3
EGG_INFO = src/aioredis_cluster.egg-info
LINT_FORMAT_PATHS = tests src dev

.PHONY: all
all: black isort flake mypy tests

.PHONY: clean
clean:
	@rm -rf build dist
	@rm -rf `find . -name __pycache__`
	@rm -f `find . -type f -name '*.py[co]'`
	@rm -rf `find . -type d -name .mypy_cache`
	@rm -rf `find . -type d -name .pytest_cache`
	@rm -rf `find . -type d -name '*.egg-info'`
	@rm -f .make-*
	@rm -rf `find . -type f -name 'Pipfile*'`

.PHONY: devel
devel:
	pip install -Ue '.[devel,aioredis]'

.PHONY: flake
flake:
	flake8 src tests dev

.PHONY: mypy
mypy:
	mypy src dev

.PHONY: tests
tests:
	pytest tests/unit_tests

.PHONY: aioredis_tests
aioredis_tests:
	pytest tests/aioredis_tests

.PHONY: isort
isort:
	isort ${LINT_FORMAT_PATHS}

.PHONY: black
black:
	black ${LINT_FORMAT_PATHS}

.PHONY: black-check
black-check:
	black --check --diff --color ${LINT_FORMAT_PATHS}

.PHONY: system_tests
system_tests:
	REDIS_CLUSTER_STARTUP_NODES=${REDIS_CLUSTER_STARTUP_NODES} pytest tests/system_tests

.PHONY: ci-test-static
ci-test-static:
	pip install -Ur tests/requirements.txt
	pip install -e .
	black --check --diff --color ${LINT_FORMAT_PATHS}
	flake8 src/aioredis_cluster tests
	mypy src/aioredis_cluster

.PHONY: ci-test
ci-test:
	pip install -Ur tests/requirements.txt
	pip install -e .
	pytest tests/unit_tests

.PHONY: ci-redis-test
ci-redis-test:
	pip install -Ur tests/requirements.txt
	pip install -e .
	REDIS_CLUSTER_STARTUP_NODES=${REDIS_CLUSTER_STARTUP_NODES} pytest tests/system_tests

.PHONY: dist
dist: clean
ifeq ($(VERSION),)
	@echo "No package version found" >&2
	@exit 1
else
	@echo "Make version file with ${VERSION}"; \
	echo "__version__ = '$(VERSION)'" > src/aioredis_cluster/_version.py
endif
	pip install -U \
		twine \
		wheel \
		build \
		setuptools
	$(PYTHON) -m build -swn .
	ls -l dist/
