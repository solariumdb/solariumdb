#!/usr/bin/env python3
"""Summarize the TRX files of a test run, and fail when io_uring did not run.

The test suite adds the io_uring environment only when the kernel lets the process create a ring.
When it cannot, the environment theories run on the default environment alone and the run is still
green. So on Linux this check requires every theory case on "default" to have an "io_uring" twin.

Usage: test-summary.py <results directory>
Writes a Markdown summary to $GITHUB_STEP_SUMMARY when it is set, and to standard output.
"""

import os
import re
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from pathlib import Path

NS = {"t": "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"}
ENVIRONMENT = re.compile(r'^(?P<test>.*)\(environment: "(?P<environment>[^"]+)"\)$')


def main() -> int:
    results = sorted(Path(sys.argv[1]).rglob("*.trx"))
    if not results:
        print(f"No TRX files under {sys.argv[1]}.", file=sys.stderr)
        return 1

    outcomes = Counter()
    by_environment = {}
    for path in results:
        for result in ET.parse(path).getroot().iterfind(".//t:UnitTestResult", NS):
            name = result.get("testName", "")
            outcomes[result.get("outcome", "Unknown")] += 1
            match = ENVIRONMENT.match(name)
            if match:
                by_environment.setdefault(match["environment"], set()).add(match["test"])

    default = by_environment.get("default", set())
    io_uring = by_environment.get("io_uring", set())
    missing = sorted(default - io_uring)

    lines = [
        "## Tests",
        "",
        "| Total | Passed | Failed | Skipped |",
        "| ---: | ---: | ---: | ---: |",
        f"| {sum(outcomes.values())} | {outcomes['Passed']} | {outcomes['Failed']} | {outcomes['NotExecuted']} |",
        "",
        "| Environment | Theory cases |",
        "| --- | ---: |",
    ]
    lines += [f"| {name} | {len(tests)} |" for name, tests in sorted(by_environment.items())]

    failed = False
    if not io_uring:
        lines += ["", "**io_uring did not run.** `IoUringStorageEnvironment.IsSupported()` returned false on this runner."]
        failed = True
    elif missing:
        lines += ["", f"**{len(missing)} theory cases ran on default but not on io_uring:**", ""]
        lines += [f"- `{test}`" for test in missing]
        failed = True

    summary = "\n".join(lines) + "\n"
    print(summary)
    if "GITHUB_STEP_SUMMARY" in os.environ:
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as file:
            file.write(summary)

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
