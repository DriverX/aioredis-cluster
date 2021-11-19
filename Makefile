.PHONY: \
	all \
	clean \
	black \
	black-check \
	flake \
	mypy \
	test \
	system_test \
	devel \
	isort \
	mypy \
	ci-test-static \
	ci-test \
	ci-redis-test \

PYTHON ?= python3
EGG_INFO = src/aioredis_cluster.egg-info

all: black isort flake mypy test

clean:
	@rm -rf build dist
	@rm -rf `find . -name __pycache__`
	@rm -f `find . -type f -name '*.py[co]'`
	@rm -rf `find . -type d -name .mypy_cache`
	@rm -rf `find . -type d -name .pytest_cache`
	@rm -rf `find . -type d -name '*.egg-info'`
	@python setup.py clean
	@rm -f .make-*
	@rm -rf `find . -type f -name 'Pipfile*'`

devel:
	pip install -Ue '.[devel]'

flake:
	flake8 setup.py src/aioredis_cluster tests

mypy:
	mypy setup.py src/aioredis_cluster

test:
	pytest tests/unit_tests

isort:
	@isort setup.py src/aioredis_cluster tests

black:
	black setup.py tests src

black-check:
	black --check --diff --color setup.py tests src

system_test:
	REDIS_CLUSTER_STARTUP_NODES=${REDIS_CLUSTER_STARTUP_NODES} pytest tests/system_tests

ci-test-static:
	pip install -Ur tests/requirements.txt
	pip install -e .
	black --check --diff --color setup.py tests src
	flake8 setup.py src/aioredis_cluster tests
	mypy setup.py src/aioredis_cluster

ci-test:
	pip install -Ur tests/requirements.txt
	pip install -e .
	pytest tests/unit_tests

ci-redis-test:
	pip install -Ur tests/requirements.txt
	pip install -e .
	REDIS_CLUSTER_STARTUP_NODES=${REDIS_CLUSTER_STARTUP_NODES} pytest tests/system_tests

dist: clean
ifeq ($(VERSION),)
	@echo "No package version found" >&2
	@exit 1
else
	@echo "Make version file with ${VERSION}"; \
	echo "__version__ = '$(VERSION)'" > src/aioredis_cluster/_version.py
endif
	pip install -U wheel
	$(PYTHON) setup.py sdist bdist_wheel
