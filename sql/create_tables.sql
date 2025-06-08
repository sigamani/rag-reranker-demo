
DROP TABLE IF EXISTS company;
DROP TABLE IF EXISTS policy;

CREATE TABLE company (
  company_id           INTEGER PRIMARY KEY,
  name                 TEXT,
  operating_jurisdiction TEXT,
  sector               TEXT,
  last_login           TIMESTAMP
);

CREATE TABLE policy (
  policy_id            INTEGER PRIMARY KEY AUTOINCREMENT,
  raw_id               TEXT    UNIQUE,
  name                 TEXT,
  geography            TEXT,
  sector               TEXT,
  published_date       DATE,
  updated_date         TIMESTAMP,
  active               BOOLEAN,
  description          TEXT,
  topics               TEXT,
  source_url           TEXT
);