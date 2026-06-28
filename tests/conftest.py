"""
conftest.py — Root pytest configuration for ValidateX test suite.

Responsibilities:
  1. Auto-detect and configure JAVA_HOME so PySpark tests work without
     manually setting environment variables before running pytest.
  2. Configure HADOOP_HOME (winutils) on Windows.
  3. Set PYSPARK_PYTHON so Spark workers use the same venv as the test runner.
  4. Gracefully skip PySpark tests if Java cannot be found anywhere.
"""

from __future__ import annotations

import os
import sys
import glob
import subprocess
from typing import Optional
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_java_home() -> Optional[str]:
    """
    Try several strategies to locate a JDK/JRE root directory:

    1. JAVA_HOME already set in environment — use it as-is.
    2. `java` binary on PATH — resolve via `java -XshowSettings:property`.
    3. Well-known Windows install directories (Microsoft OpenJDK, Oracle, etc.).
    4. Well-known Linux/macOS install directories.

    Returns the path string, or None if nothing was found.
    """
    # 1. Already set
    jh = os.environ.get("JAVA_HOME", "").strip()
    if jh and os.path.isdir(jh):
        return jh

    # 2. `java` on PATH
    try:
        out = subprocess.check_output(
            ["java", "-XshowSettings:property", "-version"],
            stderr=subprocess.STDOUT,
            timeout=10,
        ).decode(errors="replace")
        for line in out.splitlines():
            if "java.home" in line:
                jh = line.split("=", 1)[-1].strip()
                # java.home may point to jre/ sub-dir — go up one if so
                if os.path.basename(jh).lower() == "jre":
                    jh = os.path.dirname(jh)
                if os.path.isdir(jh):
                    return jh
    except Exception:
        pass

    # 3. Windows — scan common install locations
    if sys.platform == "win32":
        candidates = []
        for base in [
            r"C:\Program Files\Microsoft",
            r"C:\Program Files\Java",
            r"C:\Program Files\Eclipse Adoptium",
            r"C:\Program Files\Amazon Corretto",
        ]:
            if os.path.isdir(base):
                candidates += glob.glob(os.path.join(base, "jdk*"))
                candidates += glob.glob(os.path.join(base, "jre*"))
        # Prefer newer versions (sort descending)
        candidates.sort(reverse=True)
        for path in candidates:
            if os.path.isfile(os.path.join(path, "bin", "java.exe")):
                return path

    # 4. Linux / macOS — common paths
    for path in [
        "/usr/lib/jvm/java-21-openjdk-amd64",
        "/usr/lib/jvm/java-17-openjdk-amd64",
        "/usr/lib/jvm/java-11-openjdk-amd64",
        "/usr/lib/jvm/default-java",
        "/usr/lib/jvm/java",
        "/Library/Java/JavaVirtualMachines",
    ]:
        if os.path.isdir(path):
            return path

    return None


def _find_hadoop_home() -> Optional[str]:
    """
    Look for the .hadoop directory with winutils.exe (Windows only).
    Checks next to conftest.py and up to the project root.
    """
    if sys.platform != "win32":
        return None

    # Walk upward from this file to find .hadoop/bin/winutils.exe
    search = os.path.dirname(os.path.abspath(__file__))
    for _ in range(4):
        candidate = os.path.join(search, ".hadoop")
        if os.path.isfile(os.path.join(candidate, "bin", "winutils.exe")):
            return candidate
        search = os.path.dirname(search)

    return None


# ---------------------------------------------------------------------------
# Auto-configure environment at collection time (before any test runs)
# ---------------------------------------------------------------------------

def pytest_configure(config):
    """Called once by pytest before test collection begins."""
    java_home = _find_java_home()

    if java_home:
        os.environ["JAVA_HOME"] = java_home

        # Prepend java/bin to PATH so subprocess calls find the right java
        java_bin = os.path.join(java_home, "bin")
        path = os.environ.get("PATH", "")
        if java_bin not in path:
            os.environ["PATH"] = java_bin + os.pathsep + path

        print(f"\n[conftest] JAVA_HOME auto-set -> {java_home}")
    else:
        print(
            "\n[conftest] WARNING: Java not found - PySpark tests will error.\n"
            "  Install Java and ensure it is on PATH, or set JAVA_HOME manually."
        )

    # HADOOP_HOME (Windows only — needed for winutils.exe)
    hadoop_home = _find_hadoop_home()
    if hadoop_home:
        os.environ["HADOOP_HOME"] = hadoop_home
        print(f"[conftest] HADOOP_HOME auto-set -> {hadoop_home}")

    # Make Spark workers use the same Python interpreter as the test runner
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    # Suppress verbose Spark/Hadoop logs during tests
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
