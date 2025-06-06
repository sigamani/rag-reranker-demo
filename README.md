# Simple ETL Pipeline with PostgreSQL

This repository demonstrates a basic ETL (Extract, Transform, Load) pipeline using Python 3.10, Pandas, SQLAlchemy, and PostgreSQL. It reads data from two CSV files (`company_data.csv` and `random_policies.csv`), cleans/parses fields, and loads them into a local PostgreSQL database. We also include pytest-based tests to validate the pipeline.

---

## Table of Contents

1. [Prerequisites](#prerequisites)  
2. [Installation](#installation)  
3. [Configuration](#configuration)  
4. [Usage](#usage)  
   - [Running the ETL Pipeline](#running-the-etl-pipeline)  
   - [Verifying Loaded Data](#verifying-loaded-data)  
5. [Testing](#testing)  
6. [CI with GitHub Actions](#ci-with-github-actions)  
7. [Folder Structure](#folder-structure)  
8. [License](#license)  

---

## Prerequisites

- **Python 3.10** must be installed on your system. Pandas 1.5.2+ fully supports Python 3.10  [oai_citation:18‡pandas.pydata.org](https://pandas.pydata.org/pandas-docs/version/1.5/getting_started/install.html?utm_source=chatgpt.com).  
- A local or accessible **PostgreSQL ≥ 13** instance. You can install via Homebrew on macOS or a package manager on Linux  [oai_citation:19‡dev.to](https://dev.to/hackersandslackers/connecting-pandas-to-a-database-with-sqlalchemy-1mnf?utm_source=chatgpt.com).  
- **Git** to clone this repository.  
- (Optional) **python-venv** or another virtual environment tool to isolate dependencies  [oai_citation:20‡docs.pytest.org](https://docs.pytest.org/en/7.1.x/explanation/goodpractices.html?utm_source=chatgpt.com).

---

## Installation

1. **Clone the repository**  
   ```bash
   git clone https://github.com/yourusername/simple-etl-pipeline.git
   cd simple-etl-pipeline
