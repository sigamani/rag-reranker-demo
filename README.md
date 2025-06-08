# Maiven Practical ETL
This repository demonstrates a prototype end-to-end ETL pipeline for ingesting, validating, and storing Maiven’s policy and company data. For simplicity and because our sample sets contain only 10 and 50 rows, I chose SQLite (via sqlite3.connect) to keep everything self-contained and easily testable. In production—where we might process hundreds of thousands of records—I’d switch to PostgreSQL (psycopg2.connect) with minimal changes, since both follow the Python DB-API cursor pattern.

I layered in Pydantic validations to mirror a production-grade, Pythonic workflow and guard data integrity early. Key fields have been recast (e.g., policy_id to an 8-byte integer) and primary keys defined for efficient lookups. My error handling flags URL-resolution failures and missing fields, with a simple “green/orange/red” alert system to balance strictness against data salvageability. Full end-to-end PDF parsing and cross-document consistency checks would be next, but were outside this exercise’s scope.

I opted not to introduce Pandas—although it excels at exploratory ETL—because our data volume and structure are straightforward. A context manager plus Pydantic and structured logging has proven more than adequate. Down the road, a document store or a JSONL table might better model our dynamic sectors lists, especially as we scale into ML workflows (e.g., Huggingface’s Transformers). 

You’ll notice that much of the code here was built under the “build-fast, iterate-later” mantra—my primary goal was to use the exercise as if I were presenting to the company to motivate having a fully working prototype ETL pipeline that could be productionized in a hypothetical V2 (where there’d be handling hundreds of thousands of policies, and our retrieval logic would certainly evolve past a simple SQL join). That means you’ll see a few “hacky” and duplicated parts—like two almost-identical insert_company() / insert_policy() routines that could easily collapse into one, or some redundant validation logic in policy.py. I opted to use the latest and greatest from Pydantic v2’s new TypeAdapter API to parse both dates and datetimes in one shot (rather than stripping off trailing Z’s with regex), instead of using the older library which would have been easier. I probably over-engineered it, or implemented it inefficiently since it looks a bit monstrous for a fairly simple thing. If I had more time I’d refactor out those redundancies, consolidate validation under a single reusable adapter (if possible), and more importantly move from SQLite to something like Postgres (or a purpose-built time-series store) to handle scale and speed.

Finally, I kept a simple workflow, where everything is ran sequentially from main.py, complemented by pytest unit tests and a GitHub Actions CI workflow. I found that to be a good first step when prototyping not least that, it helps me to debug locally on my own as well catching things like [this].(https://github.com/sigamani/maiven-practical/actions/runs/15518741052/job/43689082781). Scaling out I would be very tempted to use an orchestration like Dagster (I saw it was on the tools that Maiven uses). However, that choice can add more complications so should only really be decided if operations became opaque and buggy and it was a necessity rather than a cool thing to use. 

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
