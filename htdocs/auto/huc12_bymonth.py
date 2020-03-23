#!/usr/bin/env python
"""Mapping Interface"""
import cgi
from io import BytesIO
import calendar

import numpy as np
from pyiem.util import get_dbconn, ssw


def make_plot(huc12, scenario):
    """Make the map"""
    import matplotlib

    matplotlib.use("agg")
    import matplotlib.pyplot as plt

    pgconn = get_dbconn("idep")
    cursor = pgconn.cursor()

    # Check that we have data for this date!
    cursor.execute(
        """
        WITH data as (
            SELECT extract(year from valid) as yr,
            extract(month from valid) as mo,
            sum(avg_loss) * 4.463 as val from results_by_huc12
            WHERE huc_12 = %s and scenario = %s GROUP by mo, yr)

        SELECT mo, avg(val), stddev(val), count(*) from data
        GROUP by mo ORDER by mo ASC
    """,
        (huc12, scenario),
    )
    months = []
    data = []
    confidence = []
    for row in cursor:
        months.append(row[0])
        data.append(row[1])
        confidence.append(row[2] / (row[3] ** 2))
    months = np.array(months)
    (_, ax) = plt.subplots(1, 1)
    bars = ax.bar(months - 0.4, data, color="tan", yerr=confidence)
    ax.grid(True)
    ax.set_ylabel("Soil Detachment (t/a)")
    ax.set_title(
        ("Monthly Average Soil Detachment (t/a)\nHUC12: %s") % (huc12,)
    )
    ax.set_xticks(np.arange(1, 13))
    ax.set_xticklabels(calendar.month_abbr[1:])
    ax.set_xlim(0.5, 12.5)
    ax.set_ylim(0, max(data) * 1.1)

    def autolabel(rects):
        # attach some text labels
        for rect in rects:
            height = rect.get_height()
            ax.text(
                rect.get_x() + rect.get_width() / 2.0,
                1.05 * height,
                "%.1f" % height,
                ha="center",
                va="bottom",
            )

    autolabel(bars)
    ram = BytesIO()
    plt.savefig(ram, format="png", dpi=100)
    ram.seek(0)
    return ram.read()


def main():
    """Do something fun"""
    form = cgi.FieldStorage()
    huc12 = form.getfirst("huc12", "000000000000")[:12]
    scenario = int(form.getfirst("scenario", 0))

    ssw("Content-type: image/png\n\n")
    ssw(make_plot(huc12, scenario))


if __name__ == "__main__":
    # See how we are called
    main()
