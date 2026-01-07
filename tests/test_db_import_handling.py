import pytest
import psycopg2 as ps
from unittest.mock import patch

from main import import_to_postgres


def test_import_handles_operational_error():
    # When psycopg2.connect raises OperationalError, import_to_postgres should catch it and return gracefully
    with patch("main.ps.connect", side_effect=ps.OperationalError("no network")) as mock_connect:
        # Empty schema => nothing to import, but connect is still attempted
        import_to_postgres({}, {"host": "x", "database": "y"})
        assert mock_connect.called


def test_import_accepts_url_dsn_and_calls_connect_with_dsn():
    with patch("main.ps.connect") as mock_connect:
        url = "postgresql+psycopg2://user.name:pa:ss@host:5432/db?sslmode=require"
        import_to_postgres({}, {"url": url})
        assert mock_connect.called
        # Ensure it was called with a DSN kwarg
        called_kwargs = mock_connect.call_args.kwargs
        assert "dsn" in called_kwargs
        assert called_kwargs["dsn"].startswith("postgresql://")
        # username and password should be percent-encoded so '.' and ':' are preserved
        assert "%2E" in called_kwargs["dsn"] or "%3A" in called_kwargs["dsn"]
