"""p3_00 architecture diagram for Post 3 (Bellabeat pipeline).

Three layers, left to right: orchestration (Step Functions), compute
(transient EMR cluster), storage (one S3 bucket). Every number comes
from run-1 (2026-10-14) or the committed code.
"""
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Patch

BG, INK, MUTED = "#ffffff", "#1f2933", "#52606d"
S3C, SFN, LAM, EMR, FAIL = "#2e7d32", "#ad1457", "#d35400", "#5e35b1", "#b3261e"
S3F, SFNF, EMRF, FAILF = "#eef7ee", "#fcf1f6", "#f3effb", "#fdecea"

fig, ax = plt.subplots(figsize=(13, 7.4), dpi=200)
fig.patch.set_facecolor(BG)
ax.set_xlim(0, 130)
ax.set_ylim(0, 74)
ax.axis("off")


def frame(x, y, w, h, color, fill, title, sub=None, fs=12):
    ax.add_patch(FancyBboxPatch(
        (x, y), w, h, boxstyle="round,pad=0.3,rounding_size=1.4",
        linewidth=1.8, edgecolor=color, facecolor=fill))
    ax.text(x + 1.2, y + h - 2.2, title, fontsize=fs, fontweight="bold",
            color=color, va="center")
    if sub:
        ax.text(x + 1.2, y + h - 5.0, sub, fontsize=9.5, color=MUTED,
                va="center")


def box(x, y, w, h, title, lines, color, fill=BG, tfs=11.5, sfs=10):
    ax.add_patch(FancyBboxPatch(
        (x, y), w, h, boxstyle="round,pad=0.3,rounding_size=1.0",
        linewidth=1.7, edgecolor=color, facecolor=fill))
    step = 2.6
    top = y + h / 2 + (len(lines) * step) / 2
    ax.text(x + w / 2, top, title, ha="center", va="center",
            fontsize=tfs, fontweight="bold", color=INK)
    for i, line in enumerate(lines):
        ax.text(x + w / 2, top - step * (i + 1), line, ha="center",
                va="center", fontsize=sfs, color=MUTED)


def arrow(p1, p2, color=INK, ls="-"):
    ax.add_patch(FancyArrowPatch(
        p1, p2, arrowstyle="-|>", mutation_scale=13, linewidth=1.7,
        color=color, linestyle=ls, shrinkA=0, shrinkB=0))


def text(x, y, s, color, fs=9.5, ha="center"):
    ax.text(x, y, s, ha=ha, va="center", fontsize=fs, color=color)


# ---------- title ----------
ax.text(1, 71, "Bellabeat pipeline: Step Functions orchestrating a transient EMR cluster",
        fontsize=15.5, fontweight="bold", color=INK, va="center")

# ---------- orchestration ----------
frame(0.8, 6, 57.7, 60, SFN, SFNF, "AWS Step Functions · state machine")

sx, sw, sh = 28.5, 28, 7
ys = [49, 39.5, 30, 20.5, 11]
steps = [
    ("1  InventoryFiles", ["Lambda · lists raw/ CSVs"], LAM),
    ("2  CreateCluster", ["starts the EMR cluster"], EMR),
    ("3  RunPySparkJob", ["spark-submit fitbit_etl.py"], EMR),
    ("4  ValidateOutput", ["Lambda · reads processed/"], LAM),
    ("5  TerminateCluster", ["cluster torn down, run ends"], EMR),
]
for (t, s, c), y in zip(steps, ys):
    box(sx, y, sw, sh, t, s, c)
for y1, y2 in zip(ys, ys[1:]):
    arrow((sx + sw / 2, y1 - 0.3), (sx + sw / 2, y2 + sh + 0.3))

# execution input enters at the first state
box(2.5, 45, 20, 14, "Execution input", ["bucket + 4 prefixes",
    "expected_files (4 CSVs)", "rows/users thresholds",
    "cluster type + count"], SFN, tfs=10.5, sfs=9.2)
arrow((22.9, ys[0] + 3.5), (sx - 0.3, ys[0] + 3.5))
text(25.6, ys[0] + 5.3, "start", INK, fs=8.5)

# error path for steps 3-4
fx, fw = 2.5, 20
box(fx, 26, fw, 7, "TerminateAfterFailure", ["terminates the cluster"],
    FAIL, FAILF, tfs=9.5, sfs=9.2)
box(fx, 12.5, fw, 7, "PipelineFailed", ["run ends as Failed"],
    FAIL, FAILF, tfs=10, sfs=9.2)
arrow((sx - 0.3, ys[2] + 3.5), (fx + fw + 0.3, 31), color=FAIL, ls="--")
arrow((sx - 0.3, ys[3] + 3.5), (fx + fw + 0.3, 28), color=FAIL, ls="--")
arrow((fx + fw / 2, 25.7), (fx + fw / 2, 19.8), color=FAIL, ls="--")
text(fx + fw / 2, 36, "Catch on steps 3–4", FAIL)
ax.text(fx + fw / 2, 9.2, "steps 1–2 end in their\nown Fail states",
        ha="center", va="center", fontsize=8.8, color=MUTED, linespacing=1.3)

# ---------- compute ----------
ex, ew = 70, 23
frame(ex, 11, ew, 45, EMR, EMRF, "Amazon EMR on EC2",
      "release 7.13 · Spark 3.5", fs=11)
box(ex + 2, 39, ew - 4, 6, "Primary", ["m6g.xlarge"], EMR, tfs=10.5, sfs=9.5)
box(ex + 2, 30.5, ew - 4, 6, "Core", ["m6g.xlarge"], EMR, tfs=10.5, sfs=9.5)
box(ex + 2, 22, ew - 4, 6, "Core", ["m6g.xlarge"], EMR, tfs=10.5, sfs=9.5)
ax.text(ex + ew / 2, 16, "exists only during a run\n(6 min 52 s in run-1)",
        ha="center", va="center", fontsize=9.5, color=MUTED, linespacing=1.3)

for y, lbl in ((ys[1], "create"), (ys[2], "add step"), (ys[4], "terminate")):
    arrow((sx + sw + 0.3, y + 3.5), (ex - 0.6, y + 3.5), color=EMR, ls="--")
    text(64.3, y + 5.4, lbl, EMR, fs=8.5)

# ---------- storage ----------
bx, bw = 100, 29
frame(bx, 11, bw, 45, S3C, S3F, "Amazon S3", "one bucket")
box(bx + 2, 43, bw - 4, 6, "bellabeat-raw/", ["4 Fitbit CSVs · 933 KB"],
    S3C, tfs=10.5, sfs=9.5)
box(bx + 2, 35, bw - 4, 5.5, "bellabeat-scripts/", ["fitbit_etl.py"],
    S3C, tfs=10.5, sfs=9.5)
box(bx + 2, 19.5, bw - 4, 13, "bellabeat-processed/",
    ["Parquet, partitioned by user", "daily/   868 rows",
     "hourly/  20,467 rows", "33 users in each"], S3C, tfs=10.5, sfs=9.5)
box(bx + 2, 12.5, bw - 4, 4.8, "bellabeat-logs/", [], S3C, tfs=10)

mid = (ex + ew + bx) / 2
for y, lbl, into_emr in ((46, "read", True), (37.75, "script", True),
                         (26, "write", False), (14.9, "logs", False)):
    a, b = (bx - 0.6, y), (ex + ew + 0.6, y)
    arrow(a, b, color=S3C) if into_emr else arrow(b, a, color=S3C)
    text(mid, y + 1.9, lbl, S3C, fs=8.8)

# source: uploaded to the raw prefix
box(bx, 60, bw, 7, "Kaggle Fitbit export",
    ["4 of 18 CSVs · Apr 12–May 12, 2016"], INK, tfs=10.5, sfs=9.5)
arrow((bx + bw - 6, 59.7), (bx + bw - 6, 49.4), color=S3C)
ax.text(bx + bw - 6.8, 57.6, "upload", ha="right", va="center", fontsize=8.8, color=S3C)

# ---------- legend ----------
handles = [
    Line2D([0], [0], color=INK, lw=1.8, label="next state"),
    Line2D([0], [0], color=S3C, lw=1.8, label="data"),
    Line2D([0], [0], color=EMR, lw=1.8, ls="--", label="EMR API call"),
    Line2D([0], [0], color=FAIL, lw=1.8, ls="--", label="error path"),
    Patch(facecolor=BG, edgecolor=LAM, lw=1.7, label="Lambda task"),
    Patch(facecolor=BG, edgecolor=EMR, lw=1.7, label="EMR task"),
]
ax.legend(handles=handles, loc="lower left", bbox_to_anchor=(0.005, -0.01),
          ncol=6, frameon=False, fontsize=9.5, handlelength=2.2)

fig.savefig("p3_00_architecture.png", facecolor=BG, bbox_inches="tight",
            pad_inches=0.2)
print("saved")
