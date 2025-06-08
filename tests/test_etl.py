"""
Test module for the ETL process that loads and queries relevant policies
from a local SQLite database. Uses pytest to verify that the
`relevant_policies` view is correctly populated and returns the expected
results. Guards against regressions in core ETL functionality as the
codebase evolves.
"""

import os
import sqlite3

def load_relevant_policies(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT company_id, id, geography, updated_date, avg_days_since_update
        FROM relevant_policies
        ORDER BY company_id, updated_date DESC;
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows

def test_relevant_policies_view():
    db_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'maiven.db')
    assert os.path.exists(db_path), f"Database not found at {db_path}"

    rows = load_relevant_policies(db_path)
    rounded = [
        (cid, pid, geo, upd, round(avg, 1))
        for cid, pid, geo, upd, avg in rows
    ]

    expected = [
        (3, 1434, 'DE', '2025-03-18 00:24:50', 82.6),
        (4, 1257, 'GB', '2025-03-23 08:51:27', 77.6),
        (5, 1434, 'DE', '2025-03-18 00:24:50', 82.6),
        (6, 263,  'NL', '2025-03-14 21:17:08', 86.6),
        (7, 263,  'NL', '2025-03-14 21:17:08', 86.6),
        (8, 1434, 'DE', '2025-03-18 00:24:50', 82.6),
        (9, 22,   'FR', '2025-04-03 12:55:08', 74.6),
        (9, 16,   'FR', '2025-03-18 05:56:39', 74.6),
    ]

    assert rounded == expected