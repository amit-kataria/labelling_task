from __future__ import annotations

import pytest

from labelling_task.auth.dependencies import _bearer_token
from labelling_task.errors import AuthError


def test_bearer_token_ok() -> None:
    assert _bearer_token("Bearer abc.def.ghi") == "abc.def.ghi"


@pytest.mark.parametrize("value", [None, "", "Basic xxx", "Bearer", "Bearer "])
def test_bearer_token_invalid(value) -> None:
    with pytest.raises(AuthError):
        _bearer_token(value)
