import pytest
from db import DatabaseConnector


def test_connect():
    # You could use a test database URL like SQLite in-memory for testing
    test_conn = DatabaseConnector("sqlite:///:memory:")
    connection = test_conn.connect(test_conn.database_url)
    assert connection is not None

def test_connect_type():
    # You could use a test database URL like SQLite in-memory for testing
    test_conn = DatabaseConnector("sqlite:///:memory:")
    connection = test_conn.connect(test_conn.database_url)
    assert type(connection).__name__ == "Engine"
