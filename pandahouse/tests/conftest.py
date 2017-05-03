import pytest

import numpy as np
import pandas as pd


@pytest.fixture(scope='session')
def host():
    return 'http://bdas-worker-4:8123'
    return 'https://localhost:8123'
