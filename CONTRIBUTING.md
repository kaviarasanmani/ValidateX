# Contributing to ValidateX

First off, thank you for considering contributing to ValidateX! It's people like you that make ValidateX such a great tool.

## Where do I go from here?

If you've noticed a bug or have a request for a new feature, you can [create an issue](https://github.com/kaviarasanmani/ValidateX/issues). If you want to contribute code, see the [Development Guide](#development-guide) below.

## Development Guide

### 1. Set up your environment

ValidateX is built to run on Python 3.9+. Fork the repository, then clone it locally:

```bash
git clone https://github.com/YOUR_USERNAME/ValidateX.git
cd ValidateX
```

Set up a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate  # Or `.\\.venv\\Scripts\\activate` on Windows
pip install -e ".[dev,spark]"
```

### 2. Run the tests

Before making any changes, make sure the test suite passes on your machine:

```bash
pytest tests/ -v
```

### 3. Make your changes

Whether it's a bug fix, new expectation, or documentation improvement, we appreciate it! 

If you're creating a **new expectation**, look at `validatex/expectations/column_expectations.py` for inspiration. Custom expectations just need to subclass `Expectation` and implement `_validate_pandas` (and optionally `_validate_spark`).

### 4. Run tests and linting again

Once your changes are ready, run tests and ensure coverage hasn't dropped. Also, ensure your code passes static analysis checks:

```bash
# Run tests with coverage
pytest tests/ -v --cov=validatex

# Format code with Black
black validatex tests

# Lint with Flake8
flake8 validatex tests

# Run type checker with mypy
mypy validatex
```

### 5. Submit a Pull Request

Push your branch up to your fork and submit a Pull Request against the `main` branch of ValidateX. Please fill out the PR template completely so we understand what your changes do.

Thank you!
