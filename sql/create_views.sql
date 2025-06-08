PRAGMA foreign_keys = ON;

DROP VIEW IF EXISTS relevant_policies;

CREATE VIEW relevant_policies AS
WITH
  recent_policies AS (
    SELECT
      p.id,
      p.geography,
      p.updated_date,
      p.status,
      c.company_id
    FROM policy p
    JOIN company c
      ON p.geography = c.operating_jurisdiction
    WHERE p.status = 1
      AND date(p.updated_date) >= date('now','-100 days')
  ),

  avg_past_year AS (
    SELECT
      geography,
      CAST(AVG( julianday('now') - julianday(date(updated_date)) AS INTEGER) 
        AS avg_days_since_update
    FROM policy
    WHERE status is 1
      AND date(updated_date) >= date('now','-365 days')
    GROUP BY geography
  )

SELECT
  rp.company_id,
  rp.id,
  rp.geography,
  rp.updated_date,
  apy.avg_days_since_update
FROM recent_policies rp
LEFT JOIN avg_past_year apy
  ON rp.geography = apy.geography
ORDER BY rp.company_id, rp.updated_date DESC;
