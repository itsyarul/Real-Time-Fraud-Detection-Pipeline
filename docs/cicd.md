# 🚀 CI/CD & Automated Workflows

This document clarifies the architecture separating **Software CI/CD** (GitHub Actions) from **Data & ML Continuous Training / Continuous Deployment (CT/CD)** (Apache Airflow).

---

## 🏛️ Architectural Distinction

```
                        Code Changes (PR / Commit)
                                    │
                                    ▼
                         GitHub Actions (Software CI/CD)
                                    │
           ┌────────────────────────┼────────────────────────┐
           ▼                        ▼                        ▼
    Lint & Formatting         Unit Tests (Pytest)     Docker Build & Push
  (flake8, black, isort)      (Mocked S3/MinIO)         (Push to GHCR)
                                    │
                                    ▼
                          Trivy Security Scan
                                    │
                                    ▼
                         Production Deployment
                                    │
════════════════════════════════════╪═════════════════════════════════════
                                    │
                         Data Ingestion (Streaming)
                                    │
                                    ▼
                        Apache Airflow (ML CT/CD)
                                    │
           ┌────────────────────────┼────────────────────────┐
           ▼                        ▼                        ▼
  Validate Silver Table      Rebuild ML Dataset        Retrain Model
   (Great Expectations)      (80/20 Train/Test)     (Balanced Logistic Reg)
                                    │
                                    ▼
                         Threshold Sweep & Eval
                                    │
                                    ▼
                          Compare Candidate F1
                                    │
                                    ▼
                         Promote to Production
```

- **GitHub Actions** governs the **Software Lifecycle**: Code quality, test coverage, container builds, security scans, and software releases.
- **Apache Airflow** governs the **Machine Learning Lifecycle**: Detecting data changes, running data validations, executing model retraining, tuning thresholds, and promoting models.

---

## 1. GitHub Actions Workflows (`.github/workflows/`)

| Workflow File | Trigger | Responsibility |
|---|---|---|
| `ci.yml` | Push & PR to `main`, `develop` | Runs `flake8`, `black`, `isort`, `yamllint`, and validates `docker-compose.yml` syntax. |
| `tests.yml` | Push & PR to `main`, `develop` | Executes `pytest` test suites with mocked S3/MinIO fixtures to verify Model Registry and utility classes. |
| `docker-build.yml` | Push to `main` | Builds 4 container images (Spark, Airflow, Metrics Pusher, Producer) and pushes to **GitHub Container Registry (GHCR)**. |
| `release.yml` | Git tag `v*.*.*` | Automatically drafts GitHub Releases and tags production Docker images. |
| `security-scan.yml` | Weekly cron (Mon 07:00 UTC) | Scans containers for CVE vulnerabilities via **Trivy** and audits Python dependencies via **pip-audit**. |

---

## 2. Running Local CI Checks

Before opening a pull request, run the same checks locally:

```bash
# Code formatting checks
flake8 apps airflow configs
black --check apps airflow configs
isort --check-only apps airflow configs

# Run unit tests
pytest tests/ -v
```
