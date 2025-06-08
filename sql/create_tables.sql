DROP TABLE IF EXISTS company;
DROP TABLE IF EXISTS policy;

CREATE TABLE company (
  company_id      INTEGER PRIMARY KEY,
  name            TEXT NOT NULL,
  operating_jurisdiction TEXT NOT NULL,
  sector          TEXT NOT NULL,
  last_login      TIMESTAMP NOT NULL
);

CREATE TABLE policy (
  id            TEXT PRIMARY KEY,
  name            TEXT NOT NULL,
  geography     TEXT NOT NULL,
  sectors        TEXT NOT NULL,
  published_date DATE NOT NULL,
  updated_date  TIMESTAMP NOT NULL,
  status        TEXT NOT NULL,
  description   TEXT NOT NULL,
  topics        TEXT NOT NULL,
  source_url    TEXT NOT NULL
);