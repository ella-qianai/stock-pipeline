"""Pytest bootstrap: the Lambda modules create boto3 clients at import
time (standard practice — it lets AWS reuse the client across warm
invocations instead of recreating it every call). boto3.client() doesn't
need real credentials to construct, but it does need *some* region
configured, which won't exist in a bare CI runner. Setting a dummy region
here means `pytest` can import and unit-test the pure logic in these
files without an AWS account or any real config on the machine running
the tests.
"""
import os

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
