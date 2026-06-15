# ValidateX Upcoming Features Checklist

This document tracks upcoming roadmap features and integrations for ValidateX.

- [ ] Slack & Teams Notifications on Failure
  - [ ] Implement a Slack webhook integration in `validatex.reporting`
  - [ ] Implement a Microsoft Teams webhook integration
  - [ ] Add CLI flags `--slack-webhook` and `--teams-webhook`
  - [ ] Support YAML configuration for alerts
- [ ] GitHub Action Template for CI/CD
  - [ ] Create a reusable GitHub Action under `.github/workflows/`
  - [ ] Document action usage in the user guide
- [ ] Polars Engine Support
  - [ ] Extend `Expectation` class with `_validate_polars()`
  - [ ] Port core column expectations to Polars expressions
  - [ ] Port table-level expectations to Polars
- [ ] Baseline History Tracking & Trend Charts
  - [ ] Create a local run-history database/JSON file store
  - [ ] Track quality score metrics over time
  - [ ] Embed trend line charts inside HTML report dashboards
- [ ] Great Expectations Suite Migration Tool
  - [ ] Implement a parser to import Great Expectations JSON suites
  - [ ] Translate standard GE expectation types to ValidateX equivalents
- [ ] Web Dashboard for Multi-Dataset Monitoring
  - [ ] Design a lightweight React or Streamlit dashboard
  - [ ] Support loading multiple validation reports dynamically
- [ ] dbt Integration Plugin
  - [ ] Add a dbt test wrapper to run ValidateX checks directly during `dbt test`
