"""Commonly used fields in CGI."""

from typing import Annotated

from pydantic import Field


SCENARIO_FIELD = Annotated[
    int,
    Field(
        description=("DEP scenario identifier. Typically 0 for 'production'."),
        ge=-9999,
        le=9999,
    ),
]
