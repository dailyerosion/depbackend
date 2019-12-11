#!/usr/bin/env python
"""Return the time domain that we have DEP data for, given this scenario"""
import cgi
import json
import datetime

from pyiem.util import get_dbconn, ssw

ISO = "%Y-%m-%dT%H:%M:%SZ"


def get_time(scenario):
    """Search for q"""
    pgconn = get_dbconn("idep")
    cursor = pgconn.cursor()
    d = dict()
    d["server_time"] = datetime.datetime.utcnow().strftime(ISO)
    d["first_date"] = None
    d["last_date"] = None
    d["scenario"] = scenario
    key = "last_date_%s" % (scenario,)
    cursor.execute(
        """SELECT value from properties
    WHERE key = %s""",
        (key,),
    )
    if cursor.rowcount == 1:
        row = cursor.fetchone()
        d["first_date"] = datetime.date(2007, 1, 1).strftime(ISO)
        d["last_date"] = datetime.datetime.strptime(row[0], "%Y-%m-%d").strftime(ISO)

    return d


def main():
    """DO Something"""
    form = cgi.FieldStorage()
    scenario = int(form.getfirst("scenario", 0))
    ssw("Content-type: application/json\n\n")

    ssw(json.dumps(get_time(scenario)))


if __name__ == "__main__":
    main()
