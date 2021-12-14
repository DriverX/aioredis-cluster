import platform
import re
import sys
from pathlib import Path

from setuptools import find_packages, setup


def get_requires():
    requires = [
        "dataclasses; python_version < '3.7'",
        "async-timeout",
    ]
    if platform.python_implementation() == "CPython":
        requires.append("hiredis")
    return requires


if sys.version_info < (3, 6, 5):
    raise RuntimeError("aioredis_cluster doesn't support Python version prior 3.6.5")


def get_version() -> str:
    content = Path("src/aioredis_cluster/_version.py").read_text()
    m = re.search(r'^\s*__version__\s*\=\s*[\'"]([^\'""]+)[\'"]', content, re.M)
    assert m
    return m.group(1)


def get_description() -> str:
    texts = [
        Path("README.md").read_text(encoding="utf-8"),
        Path("CHANGES.md").read_text(encoding="utf-8"),
        Path("CONTRIBUTORS.md").read_text(encoding="utf-8"),
    ]
    return "\n\n".join(texts)


setup(
    name="aioredis_cluster",
    version=get_version(),
    description="Redis Cluster support extension for aioredis",
    long_description=get_description(),
    long_description_content_type="text/markdown",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3 :: Only",
        "Operating System :: POSIX",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Framework :: AsyncIO",
    ],
    platforms=["POSIX"],
    author="Anton Ilyushenkov",
    author_email="ilyushenkov@corp.mail.ru",
    url="https://github.com/DriverX/aioredis-cluster",
    license="MIT",
    package_dir={"": "src"},
    packages=find_packages(
        "src",
        include=["aioredis_cluster", "aioredis_cluster.*"],
    ),
    install_requires=get_requires(),
    python_requires=">=3.6.5",
    include_package_data=True,
    zip_safe=False,
    extras_require={
        "devel": [
            "flake8",
            "mypy",
            "isort>=5.0.0, <6.0.0",
            "mock>=4.0.0",
            "black==22.3.0",
            "coverage",
            "pytest",
            "pytest-cov",
            # "pytest-aiohttp",
            "pytest-mock",
            "pytest-asyncio",
            "pytest-cov",
            "pytest-xdist",
            "types-dataclasses; python_version < '3.7'",
        ],
    },
)
