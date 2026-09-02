# Architecture diagram for Project 2, generated as code.
# Run from inside cyclistic-analytics/:
#   python diagram.py
# Requires: diagrams (pip/uv) and the graphviz binary.
#
# Deliberate accuracy notes:
# - S3 does NOT connect to RDS. RDS PostgreSQL cannot
#   COPY from s3://; the load was client-side \copy
#   through the workstation. A diagram showing an
#   S3 -> RDS arrow would repeat the exact error the
#   Medium post corrects.
# - Tableau connected LIVE for authoring, then extracts
#   were materialised. That is what allowed the RDS
#   instance to be deleted the same day.

from diagrams import Cluster, Diagram, Edge
from diagrams.aws.database import RDS
from diagrams.aws.storage import S3
from diagrams.onprem.client import Client
from diagrams.onprem.analytics import Tableau
from diagrams.programming.language import R

with Diagram(
    "Cyclistic Analytics - RDS PostgreSQL + R + Tableau",
    filename="screenshots/p2_02_architecture",
    show=False,
    direction="LR",
    graph_attr={"fontsize": "20", "pad": "0.4", "splines": "spline"},
):
    s3 = S3("S3 processed zone\n12 cleaned CSVs\n5,871,696 rows")

    ws = Client("workstation\npsql \\\\copy")

    with Cluster("VPC 10.20.0.0/16  (us-east-1)"):
        db = RDS(
            "RDS PostgreSQL 18.3\n"
            "db.t4g.small\n"
            "covering index\n"
            "IP-locked SG"
        )

    with Cluster("Analysis"):
        r = R("Rscript + RPostgres\nmedians, chi-squared,\ndensity plot")
        tab = Tableau("Tableau Desktop\nCustom SQL aggregates\n-> 5 extracts")

    pub = Tableau("Tableau Public\npublished dashboard")

    s3 >> Edge(label="aws s3 cp") >> ws
    ws >> Edge(label="\\\\copy over TLS") >> db
    db >> Edge(label="SELECT") >> r
    db >> Edge(label="5 aggregates") >> tab
    tab >> Edge(label="publish") >> pub
