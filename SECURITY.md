# Security Policy

## Supported Versions

We actively maintain and provide security updates for the following versions of **ValidateX**:

| Version | Supported          |
| ------- | ------------------ |
| 1.2.x   | :white_check_mark: |
| 1.1.x   | :white_check_mark: |
| < 1.1   | :x:                |

## Reporting a Vulnerability

We take the security of ValidateX seriously, especially given its role in enterprise data pipelines and warehouse integrations.

If you discover a security vulnerability, **please do not open a public GitHub issue**. Instead, report it privately via one of the following channels:

1. **GitHub Private Vulnerability Reporting:** Use the [Report a vulnerability](https://github.com/kaviarasanmani/ValidateX/security/advisories/new) button in the repository's **Security** tab.
2. **Maintainer Contact:** Open a confidential security contact request via the maintainer's GitHub profile ([@kaviarasanmani](https://github.com/kaviarasanmani)).

### What to Include in Your Report

- A clear description of the vulnerability and its potential impact.
- Affected component(s) (e.g., `validatex/expectations/`, `validatex/datasources/`, `validatex/reporting/`).
- Minimal, reproducible proof-of-concept (PoC) code or configuration.
- Suggested remediation or mitigation, if applicable.

### Response SLA

- **Initial Acknowledgment:** Within **48 hours** of receipt.
- **Triage & Severity Assessment:** Within **5 business days**.
- **Patch & Coordinated Disclosure:** Critical/High severity issues targeted within **14 days**; Medium/Low severity issues targeted in the next scheduled minor release.

---

## Security Architecture & Built-In Safeguards

ValidateX implements defense-in-depth across its configuration, execution, and reporting layers:

1. **Safe Deserialization:**
   - All YAML expectation suites, data contracts, and checkpoint configurations are parsed exclusively with `yaml.safe_load()`—arbitrary Python object instantiation (`!!python/object`) is blocked.
2. **SQL Injection Prevention (Push-Down SQL & DuckDB):**
   - Identifier validation and ANSI SQL double-quoting (`quote_sql_identifier()`) are enforced on all column and table identifiers before query execution.
   - Multi-statement injections (`;`), comment tokens (`--`, `/* */`), and DDL/DML keywords (`DROP`, `ALTER`, `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `GRANT`, `EXEC`) are rejected in SQL validation targets.
3. **HTML Report XSS Protection:**
   - All dataset values, column names, suite metadata, and error traces rendered in standalone HTML reports are escaped via `html.escape()`, and embedded `<script type="application/json">` payloads neutralize `</script>` breakout sequences.
4. **Webhook SSRF Mitigation:**
   - Outbound Slack and Microsoft Teams webhook notifications validate URL schemes (`https://` / `http://`) and reject local file or non-HTTP schemes (`file://`, `ftp://`, `javascript:`).
5. **Automated CI Security Scanning:**
   - Every push and pull request undergoes static analysis via **GitHub CodeQL**, **Bandit (SAST)**, **pip-audit (dependency vulnerability scanning)**, **Gitleaks (secret scanning)**, and weekly **Dependabot** updates.
