PRAGMA foreign_keys = ON;

-- Drop the view if it already exists
DROP VIEW IF EXISTS relevant_policies;

-- Create a view to expose “relevant policies” per company
CREATE VIEW relevant_policies AS
WITH
  -- 1) recent policies matching geo & recency
  recent_policies AS (
    SELECT
      p.policy_id,
      p.geography,
      p.updated_date,
      p.active,
      c.company_id
    FROM policy p
    JOIN company c
      ON p.geography = c.operating_jurisdiction
    WHERE p.active = 1
      AND date(p.updated_date) >= date('now','-100 days')
  ),

  -- 2) per‐geography average days‐since‐last‐update over past year
  avg_past_year AS (
    SELECT
      geography,
      AVG( julianday('now') - julianday(date(updated_date)) ) 
        AS avg_days_since_update
    FROM policy
    WHERE active = 1
      AND date(updated_date) >= date('now','-365 days')
    GROUP BY geography
  )

-- 3) final projection
SELECT
  rp.company_id,
  rp.policy_id,
  rp.geography,
  rp.updated_date,
  apy.avg_days_since_update
FROM recent_policies rp
LEFT JOIN avg_past_year apy
  ON rp.geography = apy.geography
ORDER BY rp.company_id, rp.updated_date DESC;
