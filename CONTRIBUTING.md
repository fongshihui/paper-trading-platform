# Contributing to Paper Trading Platform

Thank you for your interest in contributing to our paper trading platform! This document provides guidelines and instructions for contributing.

## Development Environment Setup

### Prerequisites
- Python 3.10+ with virtual environment
- Java 11+ and Maven
- Docker and Docker Compose
- Git

### Setup Steps

1. **Fork and Clone**
   ```bash
   git clone https://github.com/your-username/paper-trading-platform.git
   cd paper-trading-platform
   ```

2. **Python Environment**
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   pip install -r requirements-dev.txt  # Development dependencies
   ```

3. **Java Development**
   ```bash
   cd flink-jobs
   mvn clean compile
   ```

## Development Workflow

### Branch Strategy
- `main` - Production-ready code
- `develop` - Integration branch for features
- `feature/*` - Feature branches
- `bugfix/*` - Bug fix branches
- `hotfix/*` - Critical production fixes

### Pull Request Process

1. **Create a Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make Changes**
   - Follow coding standards
   - Add tests for new functionality
   - Update documentation

3. **Run Tests**
   ```bash
   # Python tests
   python -m pytest tests/ -v
   
   # Java tests
   cd flink-jobs && mvn test
   ```

4. **Submit Pull Request**
   - Target the `develop` branch
   - Include clear description of changes
   - Reference related issues
   - Ensure all tests pass

## Coding Standards

### Python Code Style
- Follow PEP 8 guidelines
- Use type hints where appropriate
- Maximum line length: 88 characters
- Use black for code formatting

### Java Code Style
- Follow Google Java Style Guide
- Use 2 spaces for indentation
- Maximum line length: 100 characters

### Documentation
- Add docstrings for all public functions/methods
- Update README for significant changes
- Include examples for new features

## Testing Guidelines

### Test Structure
- Unit tests for individual components
- Integration tests for service interactions
- End-to-end tests for complete workflow

### Running Tests
```bash
# Run all tests
python -m pytest tests/

# Run specific test file
python -m pytest tests/test_portfolio.py

# Run with coverage
python -m pytest tests/ --cov=.

# Java tests
cd flink-jobs && mvn test
```

## Commit Message Convention

Use conventional commit messages:
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `style:` - Code style changes
- `refactor:` - Code refactoring
- `test:` - Test additions/modifications
- `chore:` - Build process or auxiliary tool changes

Example:
```
feat: add moving average crossover strategy

- Implemented MA crossover in SignalJob.java
- Added configuration parameters for MA periods
- Updated documentation with new strategy details
```

## Review Process

1. **Code Review**
   - All PRs require at least one review
   - Reviewers should check for:
     - Code quality and standards
     - Test coverage
     - Documentation updates
     - Performance considerations

2. **CI/CD Checks**
   - All tests must pass
   - Code coverage should not decrease
   - Build must succeed

3. **Merge Approval**
   - PR author cannot approve their own PR
   - Requires approval from maintainers

## Getting Help

- Create an issue for bugs or feature requests
- Join our Discord/Slack channel for real-time help
- Check existing documentation and examples

## Release Process

1. **Version Bumping**
   - Update version in `pyproject.toml` and `pom.xml`
   - Follow semantic versioning (MAJOR.MINOR.PATCH)

2. **Release Notes**
   - Document changes in `CHANGELOG.md`
   - Include migration instructions if needed

3. **Tagging**
   ```bash
   git tag -a v1.0.0 -m "Release version 1.0.0"
   git push origin v1.0.0
   ```

Thank you for contributing! 🚀