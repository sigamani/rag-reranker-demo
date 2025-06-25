## Summary

Testing a RAG pipeline. Built for small datasets (10–50 rows), it uses SQLite for simplicity but is structured to allow migration to PostgreSQL in production. Data integrity is enforced via Pydantic validations, with typed fields, primary keys, and an alert system to catch common data issues. The implementation avoids Pandas to keep things lightweight and instead relies on modular Python files (`utils/company.py`, `utils/policy.py`,  `main.py`), structured logging, and context managers. There are some cleaning steps that I've neglected (i.e. removing duplicates) which has just come to mind. 


A simple retrieval system was also developed, using an embedding model to build a vector index of climate policy descriptions and retrieve relevant policies by company jurisdiction and sector. Results were optionally reranked using Anthropic’s Claude model, though the impact was limited due to sparse metadata. Vector similarity was preferred over strict SQL heuristics to enable fuzzy matching. Please click [here](https://github.com/sigamani/maiven-practical/blob/main/relevancy.py) for code.


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
  ```python
  python main.py
  ```

- **You should see terminal output confirming all records were processed, with 3 rows excluded during the cleaning step:**
  ```bash
  Companies: 100%|██████████| 10/10 [00:00<00:00, 63.50it/s]
  Policies: 100%|██████████| 50/50 [01:09<00:00,  1.39s/it]

   ETL finished in 69.67s

   Companies: 0/10 succeeded (0.0%)
     • name: 10 errors (100.0%) → RED
     • operating_jurisdiction: 10 errors (100.0%) → RED
   Policies: 0/50 succeeded (0.0%)
     • policy_id: 50 errors (100.0%) → RED
     • published_date: 50 errors (100.0%) → RED
     • topics: 50 errors (100.0%) → RED
     • source_url: 11 errors (22.0%) → RED
     • status: 7 errors (14.0%) → ORANGE
  ```
