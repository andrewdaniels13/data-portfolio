# Cyclistic Analytics - R analysis
#
# PROVENANCE, read this first:
# The original of this script was run interactively via
# Rscript and was never saved to a file. The figures it
# produced are recorded and published: pooled medians
# (casual 11.48 / member 8.75), chi-squared 10,934 with
# Cramer's V 0.0432, and the density plot committed at
# screenshots/p2_06_ride_length_density.png.
#
# This file reproduces that analysis from those recorded
# queries and outputs. It has NOT been re-executed: the
# RDS instance was deleted at the Phase 4 cost gate, so
# there is nothing left to run it against. Treat it as
# the method, not as a tested artifact.
#
# The lesson is worth more than the inconvenience:
# analysis run interactively is analysis you cannot
# commit. Everything in this project that was written to
# a file survived. The one thing typed straight into a
# session did not.

library(RPostgres)
library(ggplot2)

# Connection. Host is read from the environment along
# with the credentials - the RDS endpoint hostname names
# the account's resource-naming scheme and the region, so
# it is scrub-class and never hardcoded.
con <- dbConnect(
  Postgres(),
  host     = Sys.getenv("RDS_HOST"),
  port     = 5432,
  dbname   = Sys.getenv("RDS_DB"),
  user     = Sys.getenv("RDS_USER"),
  password = Sys.getenv("RDS_PASSWORD"),
  sslmode  = "require"
)

# ---------------------------------------------------------
# 1. Pooled duration by user type
# ---------------------------------------------------------
# This is a population query, not a sample. It exists
# because an earlier session estimated the pooled median
# as a weighted mean of per-day medians, which is not a
# valid operation - medians do not pool with weights.
# That estimate was retracted before publication.
#
# It happened to return the same 1.31x this query later
# confirmed. A method is not validated by its output
# looking plausible.

pooled <- dbGetQuery(con, "
  SELECT
    member_casual,
    COUNT(*) AS rides,
    ROUND(AVG(ride_length)::numeric, 2) AS mean_minutes,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
      (ORDER BY ride_length)::numeric, 2) AS median_minutes
  FROM cyclistic_trips
  GROUP BY member_casual
  ORDER BY member_casual;
")
print(pooled)

# Recorded output:
#   casual  2,068,884  mean 18.93  median 11.48
#   member  3,802,812  mean 12.27  median  8.75
# Mean ratio 1.54x, median ratio 1.31x. Neither is the
# "roughly twice as long" the draft claimed, and the
# median - the honest statistic given the tail - is the
# further off of the two.

# ---------------------------------------------------------
# 2. Bike type: significance vs effect size
# ---------------------------------------------------------
# COUNT(*) comes back from Postgres as bigint, which
# RPostgres returns as integer64: a double vector wearing
# an S3 class, with the integer held in the double's bit
# pattern. xtabs() strips the class, R then reads those
# bits as an IEEE double, and 607,970 silently becomes
# 3.003771e-318. No error. The failure surfaces much
# later as X-squared = NaN.
#
# as.numeric() before tabulating is the fix. This is the
# single worst silent failure in the project: a wrong
# answer off by roughly 320 orders of magnitude, with no
# warning about the actual cause.

bike <- dbGetQuery(con, "
  SELECT member_casual, rideable_type, COUNT(*) AS rides
  FROM cyclistic_trips
  GROUP BY member_casual, rideable_type
  ORDER BY member_casual, rideable_type;
")
bike$rides <- as.numeric(bike$rides)

tab <- xtabs(rides ~ member_casual + rideable_type, data = bike)
chi <- chisq.test(tab)
print(chi)

# Cramer's V. For a 2x2 table this reduces to the phi
# coefficient: sqrt(chi-squared / n).
n <- sum(tab)
cramers_v <- sqrt(as.numeric(chi$statistic) / n)
cat("Cramer's V:", round(cramers_v, 4), "\n")

# Recorded output: chi-squared = 10,934, df = 1,
# p < 2.2e-16, Cramer's V = 0.0432.
#
# Statistically certain, practically meaningless. At
# 5.87M rows a significance test is close to guaranteed
# to fire. V = 0.0432 on a 0-1 scale says the 4.2-point
# preference gap (70.61% vs 66.40% electric) is not
# something to build a campaign on. Reporting the
# p-value alone would be technically true and
# substantively misleading.

# ---------------------------------------------------------
# 3. Ride-length density by user type
# ---------------------------------------------------------
# Sampled, not population. The sample is for the picture
# only - every number published comes from a population
# query. TABLESAMPLE SYSTEM samples PAGES, not rows, and
# rows within a page are correlated, so the effective
# sample size is below the row count. The sampled member
# mean drifted 12.27 -> 12.40 (1.1%) while the medians
# barely moved: tail-sensitive statistics absorb that
# correlation first.
#
# set.seed() does NOT make this reproducible. It seeds
# R's RNG; the sampling happens server-side in Postgres,
# which ignores it. Reproducibility needs Postgres's own
# REPEATABLE (n) clause - and even that is stable only
# while the table's physical page layout is. The BOOLEAN
# cast in schema.sql rewrote the entire table; another
# rewrite would change which pages a given seed selects.
#
# No upper cap on ride_length. The draft snippet used
# WHERE ride_length <= 120, which drops real rides and
# hides the long tail that is the entire reason for
# preferring the median. Log x-axis instead.

rides <- dbGetQuery(con, "
  SELECT member_casual, ride_length
  FROM cyclistic_trips TABLESAMPLE SYSTEM (2);
")

p <- ggplot(rides, aes(x = ride_length, fill = member_casual)) +
  geom_density(alpha = 0.5) +
  scale_x_log10() +
  geom_vline(xintercept = 11.48, linetype = "dashed") +
  geom_vline(xintercept = 8.75,  linetype = "dashed") +
  labs(
    title    = "Ride length distribution by user type",
    subtitle = paste(
      "Curves from a 2% page sample; dashed lines are",
      "POPULATION medians (casual 11.48, member 8.75).",
      "Each curve normalised to area 1 independently -",
      "casual n=2,068,884, member n=3,802,812."
    ),
    x = "Minutes (log scale)", y = "Density"
  )

ggsave("p2_06_ride_length_density.png", p, width = 10, height = 6, dpi = 150)

# The separation lives in the shoulder and tail, not the
# peak. The two peaks nearly coincide; member density
# collapses after ~20 min while casual holds a bulge to
# 60 and a tail past 180. That is a stronger claim than
# "casual rides are longer" because it shows where the
# difference actually is.

dbDisconnect(con)
