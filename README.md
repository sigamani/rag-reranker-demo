## Summary

This repository showcases a prototype ETL pipeline designed to ingest, validate, and store Maiven’s climate policy and company data. Built for small-scale datasets (10–50 rows), it uses SQLite for simplicity but is structured to allow seamless migration to PostgreSQL in production. Data integrity is enforced via Pydantic validations, with typed fields, primary keys, and an alert system to catch common data issues. The implementation avoids Pandas to keep things lightweight and instead relies on modular Python files (`company.py`, `policy.py`, `etl.py`, `main.py`), structured logging, and context managers.

**Question 2:** I have answered the question by using temporary SQL views. Please click [here](https://github.com/sigamani/maiven-practical/blob/main/sql/create_views.sql).

**Question 3:** A simple retrieval system was also developed, using an embedding model to build a vector index of climate policy descriptions and retrieve relevant policies by company jurisdiction and sector. Results were optionally reranked using Anthropic’s Claude model, though the impact was limited due to sparse metadata. Vector similarity was preferred over strict SQL heuristics to enable fuzzy matching. Please click [here]([https://github.com/sigamani/maiven-practical/blob/main/sql/create_views.sql](https://github.com/sigamani/maiven-practical/blob/main/relevancy.py)) for code.

The project includes CI/CD via [GitHub Actions](https://github.com/sigamani/maiven-practical/actions) and `pytest` for testing. Though orchestration tools like Dagster were considered, they were deemed unnecessary at this stage. Evaluation prioritises recall over precision, with metrics like Precision@K and Recall@K proposed for future iterations.

## Prerequisites

- Python 3.10 
- conda or venv or whichever virtual env you prefer

## Setup

- **Clone the repo**  
   ```bash
   git clone https://github.com/your-username/maiven-practical.git
   cd maiven-practical
   ```

- **Use requirements-embeddings.txt to install dependencies needed for the Transformers-based embeddings in Q3** 
   ```bash
 	pip install -r requirements.txt
   ```
  	
- **Summary: Runs a prototype ETL that loads data from a local CSV, stores it in an in-memory SQLite database, then queries and prints the relevant policies for each company.** 

   SQLite DB.Then queries and prints out the “relevant policies by company”:**
  ```python
  python main.py
  ```

- **You should see terminal output confirming all records were processed, with 3 rows excluded during the cleaning step:**
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

I had more time, here’s what I would prioritise next:
- Refactor the codebase to simplify structure and reduce technical debt (I've half used dataclasses for the policy asset and Pydantic BaseModel for the company asset for example and the insert logic is too obtuse right now).
- Download and parse PDFs locally, avoiding HTTP requests and building a more reliable, self-contained data lake.
- Explore additional public datasets, especially those that can enrich the policy or company data such as this one [here](https://www.eea.europa.eu/en/datahub/datahubitem-view/6f1efaf1-ae32-48cb-b962-0891f84b1f5f?activeAccordion=1090804).
- Evaluate alternative database options, depending on anticipated load and business direction—e.g. PostgreSQL, a vector store, or a graph DB. For large-scale use, I’d also batch inserts or move to an ORM like SQLAlchemy.
- (Optional) Add an orchestration layer like Dagster, if pipeline complexity or reliability needs increase.
Define KPIs that align data science with business goals, such as:
 - Precision@K / Recall@K
 - NDCG / MRR
 - Cosine similarity for fuzzy matching
 - Correlation of rankings with ground truth labels (once defined)

These would help formalise what “relevance” means in this domain and guide future iterations of the system.

