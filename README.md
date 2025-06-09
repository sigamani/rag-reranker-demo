# Maiven Practical ETL
- Question 1: This repository demonstrates a prototype end-to-end ETL pipeline for ingesting, validating, and storing Maiven’s policy and company data. This prototype ETL pipeline uses SQLite for simplicity, given the small sample sizes (10–50 rows), but is structured to allow easy migration to PostgreSQL for production-scale workloads. Pydantic validations enforce data integrity early, with type casting, primary keys, and an alerting system to handle common data issues. While Pandas was excluded due to the dataset’s simplicity, structured logging, context managers, and modular Python scripts (company.py, policy.py, etl.py, main.py) ensured clarity and maintainability. The code adopts a “build fast, iterate later” approach, aiming for a functional, end-to-end pipeline that could evolve into a production-ready system. Some shortcuts—like duplicated functions and overengineered validation via Pydantic v2’s TypeAdapter—were accepted to prioritise working code over elegance. Though current inserts are row-wise, batching or an ORM like SQLAlchemy would be introduced at scale. CI/CD is handled via GitHub Actions, backed by pytest, providing a lightweight but effective foundation for testing and debugging. While orchestration tools like Dagster are familiar and powerful, they were deemed excessive for this prototype. Overall, the focus was on rapid development, modular design, and a clear path to production-readiness as data volume and system complexity grow
- Question 2: Please see here (https://github.com/sigamani/maiven-practical/blob/main/sql/create_views.sql)
- Question 3: I built a simple retrieval system using an embedding model to create a vector index of climate policy descriptions. The goal was to retrieve relevant policies for companies based on their jurisdiction and sector of operation. Given the limited volume of both policy data and simulated company data, this was primarily illustrative. To refine the retrieved results, I applied a reranking step using Anthropic’s Claude model. In practice, the benefit of this step was constrained by the sparsity of the prompts and the lack of rich metadata, so it was more exploratory than essential at this stage. I had initially considered implementing a lightweight heuristic—e.g. via SQL—based on direct geographical and sectoral matches. However, with too few sectoral matches even within the same jurisdiction, that approach didn’t generalise well. Instead, I leaned into vector similarity to enable fuzzy matching, which provided a useful signal even in sparse data scenarios. The idea was to surface semantically relevant policies even when exact attribute matches weren’t present.  If time had permitted, I’d have sourced real company data and expanded the policy dataset using more of the entries from the Climate Policy Radar dataset, which contains over 34 million rows. I explored this briefly, but the task of sourcing, cleaning, and contextualising the data is non-trivial—likely requiring dedicated effort. That said, it’s a worthwhile direction if Maiven is limited to publicly available data sources for now. Finally, in terms of evaluation, I’d prioritise recall over precision. For this use case, it’s more important to avoid missing potentially relevant policies than to return only tightly-matched results. So metrics like weighted Precision@K and Recall@K feel more appropriate as evaluation criteria. 
# Maiven Practical 

## Prerequisites

- Python 3.10 (for the relevancy.py part)
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
- Refactor the codebase to simplify structure and reduce technical debt.
- Download and parse PDFs locally, avoiding HTTP requests and building a more reliable, self-contained data lake.
- Explore additional public datasets, especially those that can enrich the policy or company data.
- Evaluate alternative database options, depending on anticipated load and business direction—e.g. PostgreSQL, a vector store, or a graph DB. For large-scale use, I’d also batch inserts or move to an ORM like SQLAlchemy.
- (Optional) Add an orchestration layer like Dagster, if pipeline complexity or reliability needs increase.
Define KPIs that align data science with business goals, such as:
 - Precision@K / Recall@K
 - NDCG / MRR
 - Cosine similarity for fuzzy matching
 - Correlation of rankings with ground truth labels (once defined)

These would help formalise what “relevance” means in this domain and guide future iterations of the system.

