"""Local test for validate_lambda.py against real Spark output.

Usage: python test_validate_local.py "<path to bellabeat-out>"

Serves the local output folder through a fake S3 client, then runs
the handler four ways: real thresholds (should pass), wrong row
count, wrong user count, and a missing _SUCCESS marker.
"""
import copy
import os
import sys

from validate_lambda import lambda_handler, OutputValidationFailed

PREFIX = "bellabeat-processed/"
GOOD = {"daily": {"rows": 868, "users": 33},
        "hourly": {"rows": 20467, "users": 33}}


class FakePaginator:
    def __init__(self, keys):
        self.keys = keys

    def paginate(self, Bucket, Prefix):
        hits = [k for k in self.keys if k.startswith(Prefix)]
        yield {"Contents": [{"Key": k} for k in hits]}


class FakeBody:
    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data


class FakeS3:
    def __init__(self, root, drop=()):
        self.files = {}
        for dirpath, _, names in os.walk(root):
            for n in names:
                full = os.path.join(dirpath, n)
                rel = os.path.relpath(full, root).replace(os.sep, "/")
                key = PREFIX + rel
                if key not in drop:
                    self.files[key] = full

    def get_paginator(self, name):
        return FakePaginator(sorted(self.files))

    def get_object(self, Bucket, Key):
        with open(self.files[Key], "rb") as f:
            return {"Body": FakeBody(f.read())}


def run(label, s3, datasets, should_fail):
    event = {"bucket": "test-bucket", "processed_prefix": PREFIX,
             "datasets": datasets}
    try:
        lambda_handler(event, None, s3=s3)
        ok = not should_fail
    except OutputValidationFailed as e:
        print("  raised: " + str(e))
        ok = should_fail
    print(("PASS " if ok else "FAIL ") + label)
    return ok


def main():
    root = sys.argv[1]
    bad_rows = copy.deepcopy(GOOD)
    bad_rows["daily"]["rows"] = 940
    bad_users = copy.deepcopy(GOOD)
    bad_users["hourly"]["users"] = 30
    no_success = FakeS3(root, drop={PREFIX + "daily/_SUCCESS"})
    results = [
        run("real thresholds", FakeS3(root), GOOD, False),
        run("wrong daily rows", FakeS3(root), bad_rows, True),
        run("wrong hourly users", FakeS3(root), bad_users, True),
        run("no daily _SUCCESS", no_success, GOOD, True),
    ]
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
