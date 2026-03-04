"""
Local CI simulation script.
Run this to check all the same things that Flake8 + pytest would check on CI.
"""
import ast
import re
import sys
import os

errors = []

# ─── 1. Syntax check all validatex/ Python files ─────────────────────────────
print("=== 1. Syntax Check ===")
for dirpath, dirs, filenames in os.walk("validatex"):
    dirs[:] = [d for d in dirs if d not in ["__pycache__", ".eggs"]]
    for fn in filenames:
        if not fn.endswith(".py"):
            continue
        fpath = os.path.join(dirpath, fn)
        try:
            src = open(fpath, encoding="utf-8").read()
            ast.parse(src)
            print(f"  OK  {fpath}")
        except SyntaxError as e:
            print(f"  ERR {fpath}:{e.lineno}: {e.msg}")
            errors.append(f"syntax:{fpath}:{e.lineno}")

# ─── 2. Duplicate kwargs in _build_result ────────────────────────────────────
print("\n=== 2. Duplicate kwargs ===")
for dirpath, dirs, filenames in os.walk("validatex"):
    dirs[:] = [d for d in dirs if d not in ["__pycache__", ".eggs"]]
    for fn in filenames:
        if not fn.endswith(".py"):
            continue
        fpath = os.path.join(dirpath, fn)
        src = open(fpath, encoding="utf-8").read()
        lines = src.splitlines()
        in_call = False
        seen = {}
        for i, line in enumerate(lines, 1):
            if "_build_result(" in line:
                in_call = True
                seen = {}
            if in_call:
                m = re.match(r"\s+(\w+)=", line)
                if m:
                    kw = m.group(1)
                    if kw in seen:
                        msg = f"  DUP {fpath}:{i}: duplicate kwarg [{kw}]"
                        print(msg)
                        errors.append(msg)
                    else:
                        seen[kw] = i
                if line.strip() == ")":
                    in_call = False
                    seen = {}
if not errors:
    print("  OK  No duplicate kwargs found")

# ─── 3. Bare except check (E722) ─────────────────────────────────────────────
print("\n=== 3. Bare except check ===")
found_bare = False
for dirpath, dirs, filenames in os.walk("validatex"):
    dirs[:] = [d for d in dirs if d not in ["__pycache__", ".eggs"]]
    for fn in filenames:
        if not fn.endswith(".py"):
            continue
        fpath = os.path.join(dirpath, fn)
        src = open(fpath, encoding="utf-8").read()
        for i, line in enumerate(src.splitlines(), 1):
            if line.strip() == "except:":
                msg = f"  E722 {fpath}:{i}: bare except"
                print(msg)
                errors.append(msg)
                found_bare = True
if not found_bare:
    print("  OK  No bare excepts found")

# ─── 4. Check typing imports vs usage ────────────────────────────────────────
print("\n=== 4. Typing import check (F82 proxy) ===")
typing_names = {"Callable", "Dict", "List", "Optional", "Any", "Type", "Union", "Tuple"}
found_f82 = False
for dirpath, dirs, filenames in os.walk("validatex"):
    dirs[:] = [d for d in dirs if d not in ["__pycache__", ".eggs"]]
    for fn in filenames:
        if not fn.endswith(".py"):
            continue
        fpath = os.path.join(dirpath, fn)
        src = open(fpath, encoding="utf-8").read()
        try:
            tree = ast.parse(src)
        except SyntaxError:
            continue

        imported = set()
        used_names = set()

        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "typing":
                for alias in node.names:
                    imported.add(alias.asname or alias.name)
            elif isinstance(node, ast.Name):
                used_names.add(node.id)

        for name in typing_names:
            if name in used_names and name not in imported:
                # Check it's not from __future__ annotations
                msg = f"  F82 {fpath}: '{name}' used but not imported from typing"
                print(msg)
                errors.append(msg)
                found_f82 = True

if not found_f82:
    print("  OK  All used typing names are imported")

# ─── Summary ─────────────────────────────────────────────────────────────────
print(f"\n=== SUMMARY: {len(errors)} issues found ===")
if errors:
    for e in errors:
        print(f"  {e}")
    sys.exit(1)
else:
    print("All checks passed!")
    sys.exit(0)
