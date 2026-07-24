from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Make the repo root importable when pytest is run from anywhere.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.transform_silver import get_spark  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    spark = get_spark("university-chapters-tests")
    yield spark
    spark.stop()
