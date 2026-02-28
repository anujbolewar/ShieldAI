from src.zscore import calculate_zscore
import pytest

def test_calculate_zscore_happy_path():
    """Verify z-score calculation with standard inputs."""
    assert calculate_zscore(value=150, mean=100, std=25) == 2.0

def test_calculate_zscore_edge_case_zero_variance():
    """Verify epsilon prevents division by zero when standard deviation is 0."""
    result = calculate_zscore(value=100, mean=100, std=0.0)
    assert result == 0.0

def test_calculate_zscore_failure_string_input():
    """Verify TypeError is raised when inputting non-numeric types."""
    with pytest.raises(TypeError):
        calculate_zscore(value="high", mean=100, std=25)
