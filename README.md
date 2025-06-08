# Maiven Practical ETL

## Prerequisites

- Python 3.10+
- `pip`
- (Optional) `virtualenv` or `conda` for isolated environments

## Setup

- **Clone the repo**  
   ```bash
   git clone https://github.com/your-username/maiven-practical.git
   cd maiven-practical
   ```

-	Install dependencies**Create & activate a virtual or conda environment** 
   ```bash
 	pip install -r requirements.txt
   ```
  	
- **Usage: Run the prototype ETL whichloads from local CSV and saves to an in-memory
   SQLite DB.Then queries and prints out the “relevant policies by company”:**
  ```python
  python main.py
  ```

- **You should see what's below in your terminal output. All records processed (with 3 rows vetoed from cleaning step):**
  ```bash
  Companies: 10/10 succeeded (100.0%)
  Policies: 50/50 succeeded (100.0%)  

  Relevant policies by company:
  company_id | policy_id | geography | updated_date         | avg_days
  -------------------------------------------------------------------
  3          | 1434      | DE        | 2025-03-18 00:24:50  | 82.5
  4          | 1257      | GB        | 2025-03-23 08:51:27  | 77.5
  ```
---

- **Notes**
The idea behind the design here was imagining how I would present a hypothetical end-to-end ETL proof-of-concept for ingesting, validating, and storing Maiven’s policy and company data (assuming it follows the structure provided). Some motivations for the choices I made are as follows: I intentionally chose SQLite because of its setup and ease of testing. It was a close call between that an postgreSQL, but postgreSQL doesn't support an in memory persistant copy, and I cant write out to a small db file locally (which was important to me in terms of the balance between ease of setup compared to what a production grade environment would look like). In a production environment handling hundreds of thousands of records, I would motivate to switch to PostgreSQL (via psycopg2.connect) which would be trivial given both follow very similar implementations. I spent some time considering the trade-offs between a relational DB, what I eventually went with or a document level (mongo-atlas, jsonl) which would lend itself well to machine learning tasks concerning text. A hybrid approach would probably be the most suitable as certain fields like "description" and "topics" are more suited to this. However I kept it simple for now seeing as I was able to do what I needed with a relational DB. I placed a lot of emphasis on data validation, where I chose to use pydantic models to validate columns. This is

Highlights
	•	Pydantic-powered validation
Mirrors a production-grade, Pythonic workflow.
	•	Recasts policy_id into an 8-byte integer
	•	Strict HttpUrl checks with URL-resolution alerts
	•	“Green/Orange/Red” severity flags guard against over- or under-strict filtering
	•	Schema & performance considerations
	•	Defined primary keys for O(1) lookups
	•	Row-by-row inserts are fine for our sample size; in V2 we’d batch or use an ORM like SQLAlchemy
	•	Dynamic fields (e.g. sector lists) are stored as JSON strings today; a document store or JSONL table may suit ML workflows better
	•	“Build-fast, iterate-later” mindset
You’ll spot some intentional trade-offs:
	•	Near-duplicate insert_company() / insert_policy() routines (candidates for consolidation)
	•	Over-engineered date/datetime parsing via Pydantic v2 TypeAdapters (regex-free, but verbose)
	•	Modular code structure
	•	company.py, policy.py, etl.py, main.py each own a clear responsibility
	•	Easily extendable as new requirements emerge
	•	CI/CD & testing
	•	GitHub Actions workflow runs main.py end-to-end and enforces a custom pytest suite
	•	Captures failures early—including parsing regressions and view-definition changes
	•	View latest CI run

Next Steps
	1.	Scale-Up
	•	Migrate to PostgreSQL (or specialized time-series store)
	•	Batch loads or switch to an ORM for high-volume inserts
	2.	Refactor
	•	DRY up redundant insert routines
	•	Consolidate Pydantic adapters for date parsing
	3.	Orchestration
	•	Evaluate Dagster or Airflow for robust pipeline scheduling
	4.	Deep Data QA
	•	Full PDF parsing → extract & cross-validate metadata
	•	Schema evolution & data lineage tracking
