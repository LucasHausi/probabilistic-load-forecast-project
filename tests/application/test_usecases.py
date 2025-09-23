from unittest.mock import Mock
from datetime import datetime
from probabilistic_load_forecast.domain.model import Measurement
from probabilistic_load_forecast.infrastructure.cds.repository import CDSRepository

def test_repository_calls_fetcher_and_mapper():
    # Arrange
    fake_measurements = [Measurement("2025-01-01T00:00", 273.15)]
    
    mock_fetcher = Mock()
    mock_fetcher.fetch.return_value = ["out.nc"]

    mock_mapper = Mock()
    mock_mapper.map.return_value = fake_measurements

    repo = CDSRepository(fetcher=mock_fetcher, mapper=mock_mapper)

    start = datetime(2025, 1, 1)
    end = datetime(2025, 1, 2)

    # Act
    result = repo.get_data(start, end)

    # Assert
    mock_fetcher.fetch.assert_called_once()
    mock_mapper.map.assert_called_once()
    assert list(result) == fake_measurements