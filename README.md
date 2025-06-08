# Maiven Practical ETL

This repository contains an end-to-end ETL pipeline that ingests company and policy CSV data, loads them into a local SQLite database, defines a `relevant_policies` view, and exposes a CLI to run the process and query “relevant policies” for a job-interview exercise. It also includes pytest-based unit tests and a GitHub Actions workflow that will fail if tests do not pass.

## Prerequisites

- Python 3.10+
- `pip`
- (Optional) `virtualenv` or `conda` for isolated environments

## Setup

- **Clone the repo**  
   ```bash
   git clone https://github.com/your-username/maiven-practical.git
   cd maiven-practical```

-	Install dependencies**Create & activate a virtual or conda environment** 
   ```bash pip install -r requirements.txt```
  	
- **Usage: Run the prototype ETL whichloads from local CSV and saves to an in-memory
   SQLite DB.Then queries and prints out the “relevant policies by company”:**
  ```python main.py```

- **You should see what's below in your terminal output:**
  ```bash
  Companies: 10/10 succeeded (100.0%)
  Policies: 50/50 succeeded (100.0%)

  Relevant policies by company:
  company_id | policy_id | geography | updated_date         | avg_days
  -------------------------------------------------------------------
  3          | 1434      | DE        | 2025-03-18 00:24:50  | 82.5
  4          | 1257      | GB        | 2025-03-23 08:51:27  | 77.5
  ```
and so on ...
