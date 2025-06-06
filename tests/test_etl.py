import os
import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from main import clean_data, load_to_db

@pytest.fixture(scope="session")
def engine():
    """Create a temporary database engine for testing."""
    return create_engine("sqlite:///:memory:")

@pytest.fixture
def sample_df_company(tmp_path):
    """Write a small CSV and return DataFrame for testing clean_data()."""
    csv_path = tmp_path / "company_test.csv"
    data = {
        "company_id": [1, 2],
        "name": ["A Corp", "B LLC"],
        "last_login": ["2025-01-01T12:00:00Z", "2025-01-02T13:30:00Z"]
    }
    pd.DataFrame(data).to_csv(csv_path, index=False)
    return clean_data(str(csv_path))

def test_clean_data(sample_df_company):
    """Ensure clean_data returns a non-empty DataFrame."""
    assert isinstance(sample_df_company, pd.DataFrame)
    assert sample_df_company.shape[0] == 2

def test_load_to_db(engine, sample_df_company):
    """Ensure that DataFrame loads into the engine without errors."""
    load_to_db(sample_df_company, "company_test", engine)
    # Verify table exists and has correct number of rows

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM company_test"))
        assert result.scalar() == 2
