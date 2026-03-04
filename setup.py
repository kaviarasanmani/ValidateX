"""ValidateX - A powerful data quality validation framework."""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="validatex",
    version="1.2.0",
    author="Kaviarasan Mani",
    description="A powerful data quality validation framework inspired by Great Expectations",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kaviarasanmani/ValidateX",
    packages=find_packages(exclude=["tests*", "examples*"]),
    include_package_data=True,
    package_data={
        "validatex": ["reporting/templates/*.html"],
    },
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.3.0",
        "pyyaml>=6.0",
        "jinja2>=3.0",
        "click>=8.0",
        "rich>=12.0",
        "colorama>=0.4.0",
    ],
    extras_require={
        "spark": ["pyspark>=3.0.0"],
        "database": ["sqlalchemy>=1.4.0"],
        "all": [
            "pyspark>=3.0.0",
            "sqlalchemy>=1.4.0",
        ],
        "dev": [
            "pytest>=7.0",
            "pytest-cov>=4.0",
            "black>=22.0",
            "flake8>=5.0",
            "mypy>=1.0.0",
            "ruff>=0.1.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "validatex=validatex.cli.main:cli",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Quality Assurance",
        "Topic :: Database",
    ],
)
