"""Generate a PDF Report for a given HUC12."""

import calendar
from datetime import datetime
from io import BytesIO

import httpx
import pandas as pd
from metpy.units import units
from pydantic import Field
from pyiem.database import get_dbconn, get_sqlalchemy_conn
from pyiem.webutil import CGIModel, iemapp
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from depbackend.auto.huc12_slopes import make_plot
from depbackend.auto.mapper import make_overviewmap

PAGE_WIDTH = letter[0]
PAGE_HEIGHT = letter[1]
GENTIME = datetime.now().strftime("%B %-d %Y")
INTROTEXT = (
    "The Daily Erosion Project generates estimates of sheet and rill "
    "erosion.  This PDF summarizes our model results.  All results should be "
    "considered preliminary and subject to future revision as improvements "
    "are made to our model system and input datasets."
)
LOCALIZATION = {
    "F1": [
        "Figure 1: Regional view with HUC8 highlighted in tan.",
        (
            "Figure 1: Regional view with HUC8 highlighted in tan "
            "and HUC12 in red."
        ),
    ],
    "F2": [
        "Figure 2: Zoomed in view of HUC8.",
        (
            "Figure 2: Zoomed in view with HUC8 highlighted in tan "
            "and HUC12 in red."
        ),
    ],
}


class Schema(CGIModel):
    """See how we are called."""

    huc: str = Field(
        "070801050306",
        description="HUC12 to generate report for",
        min_length=8,
        max_length=12,
    )


def m2f(val):
    """Convert meters to feet."""
    return ((val * units("m")).to(units("feet"))).m


def generate_run_metadata(huc12):
    """Information about DEP modelling of this huc12."""
    styles = getSampleStyleSheet()
    res = []
    pgconn = get_dbconn("idep")
    # Get the number of runs
    cursor = pgconn.cursor()
    cursor.execute(
        """
    select count(*), min(ST_Length(geom)), avg(ST_Length(geom)),
    max(ST_Length(geom)) from flowpaths
    where huc_12 = %s and scenario = 0
    """,
        (huc12,),
    )
    row = cursor.fetchone()
    res.append(
        Paragraph(
            (
                "The Daily Erosion Project models %s hill slopes within "
                "this HUC12."
                "These slopes range in length from %.1f to %.1f meters "
                "(%.1f to %.1f feet) with an overall average of %.1f meters "
                "(%.1f feet)."
            )
            % (
                row[0],
                row[1],
                row[3],
                m2f(row[1]),
                m2f(row[3]),
                row[2],
                m2f(row[2]),
            ),
            styles["Normal"],
        )
    )

    # Something about managements and crops
    rows = [["Year", "Corn", "Soybean", "Pasture", "Other"]]
    for year in range(2016, 2023):
        with get_sqlalchemy_conn("idep") as conn:
            df = pd.read_sql(
                """
            select substr(landuse, %s, 1) as datum, count(*) from
            flowpath_ofes o JOIN flowpaths f on (o.flowpath = f.fid)
            WHERE f.huc_12 = %s and f.scenario = 0 GROUP by datum
            """,
                conn,
                params=(year - 2007 + 1, huc12),
                index_col="datum",
            )
        total = df["count"].sum()
        leftover = total
        rows.append([year])
        for cropcode in ["C", "B", "P"]:
            if cropcode in df.index:
                val = df.loc[cropcode, "count"]
                leftover -= val
                rows[-1].append("%.1f%%" % (val / total * 100.0,))
            else:
                rows[-1].append("None")
        rows[-1].append("%.1f%%" % (leftover / total * 100.0,))

    # Histogram of slope profiles
    tablestyle = TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")])
    res.append(
        Table(
            [
                [
                    [
                        Image(
                            make_plot(huc12, 0),
                            width=3.6 * inch,
                            height=2.4 * inch,
                        ),
                        Paragraph(
                            (
                                "Figure 3: This plot shows the most dominate "
                                "combination of slope length and general "
                                "steepness in the watershed."
                            ),
                            styles["Normal"],
                        ),
                    ],
                    [
                        Spacer(inch, inch * 0.25),
                        Paragraph("Cropping Systems", styles["Heading3"]),
                        Table(rows),
                        Paragraph(
                            (
                                "Table 1: Estimated cropping percentage based "
                                "on modelled hillslopes.  The 'Other' column "
                                "represents all other cropping types "
                                "supported by DEP."
                            ),
                            styles["Normal"],
                        ),
                    ],
                ]
            ],
            style=tablestyle,
        )
    )

    return res


def generate_monthly_summary_table(huc12):
    """Make a table of monthly summary stats."""
    data = []
    data.append(
        [
            "Year",
            "Month",
            "Precip",
            "Runoff",
            "Loss",
            "Delivery",
            '2+" Precip',
            "Events",
        ]
    )
    data.append(
        [
            "",
            "",
            "[inch]",
            "[inch]",
            "[tons/acre]",
            "[tons/acre]",
            "[days]",
            "[days]",
        ]
    )
    huc12col = "huc_12"
    if len(huc12) == 8:
        huc12col = "substr(huc_12, 1, 8)"
    with get_sqlalchemy_conn("idep") as conn:
        df = pd.read_sql(
            f"""
        WITH data as (
            SELECT extract(year from valid)::int as year,
            extract(month from valid)::int as month, huc_12,
            (sum(qc_precip) / 25.4)::numeric as sum_qc_precip,
            (sum(avg_runoff) / 25.4)::numeric as sum_avg_runoff,
            (sum(avg_loss) * 4.463)::numeric as sum_avg_loss,
            (sum(avg_delivery) * 4.463)::numeric as sum_avg_delivery,
            sum(case when qc_precip >= 50.8 then 1 else 0 end) as pdays,
            sum(case when avg_loss > 0 then 1 else 0 end) as events
            from results_by_huc12 WHERE scenario = 0 and {huc12col} = %s
            and valid >= '2016-01-01'
            GROUP by year, month, huc_12)
        SELECT year, month,
        round(avg(sum_qc_precip), 2),
        round(avg(sum_avg_runoff), 2),
        round(avg(sum_avg_loss), 2),
        round(avg(sum_avg_delivery), 2),
        round(avg(pdays)::numeric, 1),
        round(avg(events)::numeric, 1)
        from data GROUP by year, month ORDER by year, month
        """,
            conn,
            params=(huc12,),
            index_col=None,
        )
    for _, row in df.iterrows():
        vals = [int(row["year"]), calendar.month_abbr[int(row["month"])]]
        vals.extend(["%.2f" % (f,) for f in list(row)[2:-2]])
        vals.extend(["%.0f" % (f,) for f in list(row)[-2:]])
        data.append(vals)
    data[-1][1] = "%s*" % (data[-1][1],)
    totals = df.iloc[:-1].mean()
    vals = ["", "Average"]
    vals.extend(["%.2f" % (f,) for f in list(totals[2:])])
    data.append(vals)

    style = TableStyle(
        [
            ("LINEBELOW", (2, 1), (-1, 1), 0.5, "#000000"),
            ("LINEAFTER", (1, 2), (1, -2), 0.5, "#000000"),
            ("LINEABOVE", (2, -1), (-1, -1), 0.5, "#000000"),
            ("ALIGN", (0, 0), (-1, -1), "RIGHT"),
        ]
    )
    for rownum in range(3, len(data) + 1, 2):
        style.add("LINEBELOW", (0, rownum), (-1, rownum), 0.25, "#EEEEEE")
    return Table(data, style=style, repeatRows=2)


def generate_summary_table(huc12):
    """Make a table summarizing our results, please."""
    data = []
    data.append(
        [
            "Year",
            "Precip",
            "Runoff",
            "Loss",
            "Delivery",
            '2+" Precip',
            "Events",
        ]
    )
    data.append(
        [
            "",
            "[inch]",
            "[inch]",
            "[tons/acre]",
            "[tons/acre]",
            "[days]",
            "[days]",
        ]
    )
    huc12col = "huc_12"
    if len(huc12) == 8:
        huc12col = "substr(huc_12, 1, 8)"
    with get_sqlalchemy_conn("idep") as conn:
        df = pd.read_sql(
            f"""
        WITH data as (
            SELECT extract(year from valid)::int as year, huc_12,
            (sum(qc_precip) / 25.4)::numeric as sum_qc_precip,
            (sum(avg_runoff) / 25.4)::numeric as sum_avg_runoff,
            (sum(avg_loss) * 4.463)::numeric as sum_avg_loss,
            (sum(avg_delivery) * 4.463)::numeric as sum_avg_delivery,
            sum(case when qc_precip >= 50.8 then 1 else 0 end) as pdays,
            sum(case when avg_loss > 0 then 1 else 0 end) as events
            from results_by_huc12 WHERE scenario = 0 and {huc12col} = %s
            and valid >= '2008-01-01'
            GROUP by year, huc_12)
        SELECT year,
        round(avg(sum_qc_precip), 2),
        round(avg(sum_avg_runoff), 2),
        round(avg(sum_avg_loss), 2),
        round(avg(sum_avg_delivery), 2),
        round(avg(pdays)::numeric, 1),
        round(avg(events)::numeric, 1)
        from data GROUP by year ORDER by year
        """,
            conn,
            params=(huc12,),
            index_col="year",
        )
    for year, row in df.iterrows():
        vals = [year]
        vals.extend(["%.2f" % (f,) for f in list(row)[:-2]])
        vals.extend(["%.0f" % (f,) for f in list(row)[-2:]])
        data.append(vals)
    data[-1][0] = "%s*" % (data[-1][0],)
    totals = df.iloc[:-1].mean()
    vals = ["Average"]
    vals.extend(["%.2f" % (f,) for f in list(totals)])
    data.append(vals)

    style = TableStyle(
        [
            ("LINEBELOW", (1, 1), (-1, 1), 0.5, "#000000"),
            ("LINEAFTER", (0, 2), (0, -2), 0.5, "#000000"),
            ("LINEABOVE", (1, -1), (-1, -1), 0.5, "#000000"),
            ("ALIGN", (0, 0), (-1, -1), "RIGHT"),
        ]
    )
    for rownum in range(3, len(data) + 1, 2):
        style.add("LINEBELOW", (0, rownum), (-1, rownum), 0.25, "#EEEEEE")
    return Table(data, style=style)


def draw_header(canvas, doc, huc12):
    """Do our header stuff"""
    canvas.saveState()
    canvas.drawImage(
        "/opt/depbackend/htdocs/images/logo_horiz_white.png",
        inch * 1.0,
        PAGE_HEIGHT - 100,
    )
    canvas.setFont("Times-Bold", 16)
    canvas.drawString(
        PAGE_WIDTH * 0.35,
        PAGE_HEIGHT - inch + 20,
        "Daily Erosion Project HUC%s Summary Report" % (len(huc12),),
    )
    canvas.drawString(
        PAGE_WIDTH * 0.35,
        PAGE_HEIGHT - inch,
        "HUC%s: %s" % (len(huc12), huc12),
    )
    canvas.drawString(
        PAGE_WIDTH * 0.35, PAGE_HEIGHT - inch - 20, "Generated %s" % (GENTIME,)
    )
    canvas.setFont("Times-Roman", 9)
    canvas.drawCentredString(
        PAGE_WIDTH * 0.5, 0.75 * inch, "Page %s" % (doc.page,)
    )
    canvas.restoreState()


@iemapp(help=__doc__, schema=Schema)
def application(environ, start_response):
    """See how we are called"""
    huc12 = environ["huc"]
    ishuc12 = len(huc12) == 12
    bio = BytesIO()
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(bio, pagesize=letter, topMargin=(inch * 1.5))
    story = []
    story.append(Paragraph(INTROTEXT, styles["Normal"]))
    story.append(Spacer(inch, inch * 0.25))
    story.append(Paragraph("Geographic Location", styles["Heading1"]))
    image1 = Image(
        make_overviewmap({"overview": 1, "huc": huc12, "zoom": 250}),
        width=3.6 * inch,
        height=2.4 * inch,
    )
    image2 = Image(
        make_overviewmap({"overview": 1, "huc": huc12, "zoom": 11}),
        width=3.6 * inch,
        height=2.4 * inch,
    )

    story.append(
        Table(
            [
                [
                    [
                        image1,
                        Paragraph(
                            LOCALIZATION["F1"][int(ishuc12)], styles["Normal"]
                        ),
                    ],
                    [
                        image2,
                        Paragraph(
                            LOCALIZATION["F2"][int(ishuc12)], styles["Normal"]
                        ),
                    ],
                ]
            ]
        )
    )
    story.append(Spacer(inch, inch * 0.25))
    story.append(Paragraph("DEP Input Data", styles["Heading1"]))
    story.extend(generate_run_metadata(huc12))

    story.append(PageBreak())
    story.append(Spacer(inch, inch * 0.25))
    story.append(Paragraph("Yearly Summary", styles["Heading1"]))
    story.append(generate_summary_table(huc12))
    story.append(
        Paragraph(
            (
                "Table 2: Average value does not include the "
                "current year. Events column are the number of "
                "days with non-zero soil loss. "
                "(* year to date total)"
            ),
            styles["Normal"],
        )
    )
    story.append(PageBreak())
    story.append(Spacer(inch, inch * 0.25))
    story.append(Paragraph("Monthly Summary", styles["Heading1"]))
    story.append(generate_monthly_summary_table(huc12))
    story.append(
        Paragraph(
            (
                "Table 3: Monthly Totals. Events column are the number of "
                "days with non-zero soil loss. (* month to date total)"
            ),
            styles["Normal"],
        )
    )

    def pagecb(canvas, doc):
        """Proxy to our draw_header func"""
        draw_header(canvas, doc, huc12)

    doc.build(story, onFirstPage=pagecb, onLaterPages=pagecb)
    start_response("200 OK", [("Content-type", "application/pdf")])
    return [bio.getvalue()]
