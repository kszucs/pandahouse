import pytest

from pandahouse.utils import escape


@pytest.mark.parametrize('value,quote,expected', [
    (1, '`', 1),
    (None, '`', None),
    ('\'', '`', '`\\\'`'),
    ('\t\nstring', '\'', "'\\t\\nstring'")
])
def test_escape(value, quote, expected):
    assert escape(value, quote) == expected
