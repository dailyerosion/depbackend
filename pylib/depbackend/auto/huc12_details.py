""".. title:: Provide HUC12 details used by DEP Map App.

Replaces legacy / hacky HTML generation done by /huc12-details.php

Changelog
---------

- 2025-12-03: Initial Implementation

Examples
--------

Provide details for 070801050902 Ballard Creek in english units for May 2020

https://depbackend.agron.iastate.edu/auto/huc12_details.py?\
huc12=070801050902&date=2020-05-01&date2=2020-05-31&scenario=0&metric=0

"""

from datetime import date as dateobj

import pandas as pd
import simplejson as json
from pydantic import Field
from pyiem.database import sql_helper, with_sqlalchemy_conn
from pyiem.dep import RAMPS
from pyiem.util import utc
from pyiem.webutil import CGIModel, iemapp
from sqlalchemy.engine import Connection


class Schema(CGIModel):
    """See how we are called."""

    huc12: str = Field(
        ...,
        description="HUC12 Identifier",
        pattern="^\d{12}$",
    )
    date: dateobj = Field(
        ...,
        description="Start Date",
    )
    date2: dateobj = Field(default=None, description="Inclusive End Date.")
    scenario: int = Field(
        default=0,
        description="Scenario ID",
    )
    metric: bool = Field(
        default=False,
        description="Whether to return metric units",
    )


def get_huc12name(
    conn: Connection,
    huc12: str,
    scenario: int,
) -> str:
    """Get HUC12 name."""
    res = conn.execute(
        sql_helper(
            """
        SELECT name from huc12 WHERE huc_12 = :huc12 and scenario = :scenario
        """
        ),
        {"huc12": huc12, "scenario": scenario},
    ).fetchone()
    if res is None:
        return "Unknown HUC12"
    return res[0]


@with_sqlalchemy_conn("idep")
def generate_data(environ: dict, conn: Connection | None = None) -> dict:
    """Do work"""
    payload = {
        "name": get_huc12name(conn, environ["huc12"], environ["scenario"]),
        "qc_precip": 0,
        "avg_runoff": 0,
        "avg_loss": 0,
        "avg_delivery": 0,
        "punit": "mm",
        "lunit": "tonne/ha",
        "top10": [],
    }
    dtlimit = "valid = :date"
    if environ["date2"] is not None:
        dtlimit = "valid >= :date and valid <= :date2"
    resultsdf = pd.read_sql(
        sql_helper(
            """
    select sum(qc_precip) as qc_precip,
    sum(avg_runoff) as avg_runoff, sum(avg_loss) as avg_loss,
    sum(avg_delivery) as avg_delivery from results_by_huc12 WHERE 
    {dtlimit} and huc_12 = :huc12
    and scenario = :scenario
        """,
            dtlimit=dtlimit,
        ),
        conn,
        params={
            "date": environ["date"],
            "date2": environ["date2"],
            "huc12": environ["huc12"],
            "scenario": environ["scenario"],
        },
        index_col=None,
    )
    if not resultsdf.empty:
        row = resultsdf.iloc[0]
        payload["qc_precip"] = float(row["qc_precip"] or 0)
        payload["avg_runoff"] = float(row["avg_runoff"] or 0)
        # kg/m2 to tonnes/hectare
        payload["avg_loss"] = float(row["avg_loss"] or 0) * 10.0
        payload["avg_delivery"] = float(row["avg_delivery"] or 0) * 10.0
        if not environ["metric"]:
            # Convert to inches and pounds
            payload["qc_precip"] /= 25.4
            payload["avg_runoff"] /= 25.4
            # back out the *10 above
            payload["avg_loss"] *= 0.4463
            payload["avg_delivery"] *= 0.4463
            payload["punit"] = "inch"
            payload["lunit"] = "ton/acre"

    top10 = pd.read_sql(
        sql_helper("""
    select valid, qc_precip, avg_loss, avg_delivery, avg_runoff
    from results_by_huc12 WHERE
    huc_12 = :huc12 and scenario = :scenario and avg_loss > 0
    ORDER by avg_loss DESC LIMIT 10
        """),
        conn,
        params={
            "huc12": environ["huc12"],
            "scenario": environ["scenario"],
        },
        index_col=None,
    )
    for _, row in top10.iterrows():
        qc_precip = float(row["qc_precip"] or 0)
        avg_loss = float(row["avg_loss"] or 0) * 10.0
        avg_delivery = float(row["avg_delivery"] or 0) * 10.0
        avg_runoff = float(row["avg_runoff"] or 0)
        if not environ["metric"]:
            qc_precip /= 25.4
            avg_loss *= 0.4463
            avg_delivery *= 0.4463
        payload["top10"].append(
            {
                "date": row["valid"].strftime("%Y-%m-%d"),
                "qc_precip": qc_precip,
                "avg_loss": avg_loss,
                "avg_delivery": avg_delivery,
                "avg_runoff": avg_runoff,
            }
        )

    return payload


@iemapp(help=__doc__, schema=Schema)
def application(environ, start_response):
    """Do Fun things"""
    data = generate_data(environ)
    headers = [("Content-Type", "application/json")]
    start_response("200 OK", headers)
    return json.dumps(data)
