-- Drop existing tables
DROP TABLE IF EXISTS policy;
DROP TABLE IF EXISTS company;

-- Create company table
CREATE TABLE company (
  company_id INTEGER PRIMARY KEY,
  name TEXT,
  operating_jurisdiction TEXT,
  sector TEXT,
  last_login TIMESTAMP
);

-- Create policy table
CREATE TABLE policy (
  policy_id TEXT PRIMARY KEY,
  name TEXT,
  geography TEXT,
  sector TEXT,
  published_date DATE,
  updated_date TIMESTAMP,
  active BOOLEAN,
  description TEXT,
  topics TEXT,
  source_url TEXT
);