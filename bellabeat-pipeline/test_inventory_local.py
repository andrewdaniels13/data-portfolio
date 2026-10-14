"""Local test for inventory_lambda.py against a real folder.

Usage: python test_inventory_local.py "<path to Fitbit CSV folder>"

Builds a fake S3 listing from the files in the folder, then runs
the handler three ways: all files present, one missing, one empty.
"""
import os
import sys

from inventory_lambda import lambda_handler, MissingInputFiles

PREFIX = "bellabeat-raw/"
EXPECTED = [
    "dailyActivity_merged.csv",
    "sleepDay_merged.csv",
    "hourlySteps_merged.csv",
    "weightLogInfo_merged.csv",
]


class FakePaginator:
    def __init__(self, objects):
        self.objects = objects

    def paginate(self, **kwargs):
        yield {"Contents": self.objects}


class FakeS3:
    def __init__(self, objects):
        self.objects = objects

    def get_paginator(self, name):
        return FakePaginator(self.objects)


def listing(folder):
    objs = []
    for name in sorted(os.listdir(folder)):
        path = os.path.join(folder, name)
        if os.path.isfile(path) and name.endswith(".csv"):
            size = os.path.getsize(path)
            objs.append({"Key": PREFIX + name, "Size": size})
    return objs


def run(label, objects, should_fail):
    event = {"bucket": "test-bucket", "raw_prefix": PREFIX,
             "expected_files": EXPECTED}
    try:
        lambda_handler(event, None, s3=FakeS3(objects))
        ok = not should_fail
    except MissingInputFiles:
        ok = should_fail
    print(("PASS " if ok else "FAIL ") + label)
    return ok


def main():
    objs = listing(sys.argv[1])
    print(f"csv files in folder: {len(objs)}")
    no_daily = [o for o in objs
                if not o["Key"].endswith(EXPECTED[0])]
    zero_sleep = [dict(o, Size=0)
                  if o["Key"].endswith(EXPECTED[1]) else o
                  for o in objs]
    results = [
        run("all present", objs, should_fail=False),
        run("daily missing", no_daily, should_fail=True),
        run("sleep empty", zero_sleep, should_fail=True),
    ]
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
