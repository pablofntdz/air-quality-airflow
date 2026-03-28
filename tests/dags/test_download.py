import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
import tempfile
from include.src.download_data import download_file, save_raw_file, download_data
import requests as req

def test_download_file_success():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b"test content"

    with patch("src.download_data.requests.get", return_value=mock_response):
        response = download_file("https://fake-url.com/data.csv")
        assert response.content == b"test content"


def test_download_file_retries_on_failure():
    with patch("src.download_data.requests.get") as mock_get:
        mock_get.side_effect = req.RequestException("Connection error")
        with pytest.raises(req.RequestException):
            download_file("https://fake-url.com/data.csv", retries=2, delay=0)
        assert mock_get.call_count == 2


def test_save_raw_file():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "test.csv"
        save_raw_file(b"col1,col2\n1,2", path)
        assert path.exists()
        assert path.read_bytes() == b"col1,col2\n1,2"


def test_download_data():
    mock_response = MagicMock()
    mock_response.content = b"col1,col2\n1,2"

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "test.csv"
        with patch("src.download_data.requests.get", return_value=mock_response):
            download_data("https://fake-url.com/data.csv", path)
            assert path.exists()
            assert path.read_bytes() == b"col1,col2\n1,2"