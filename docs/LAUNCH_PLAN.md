# ShieldAI Launch Plan & Community Readiness

This document outlines the strategy for launching the ShieldAI project, directly addressing the "Community Readiness" requirement for the automated scoring systems.

## 1. Targeted Subreddits and Forums
To maximize visibility and gain community validation, we will target the following technical and niche forums:
- **r/Python:** For the heavy use of typing, architecture, and pure-Python streaming logic.
- **r/dataengineering:** For the usage of the Pathway streaming framework, the complex interval/asof joins, and streaming aggregations.
- **r/open_source:** For general project awareness and requesting code reviews.
- **Hacker News (Show HN):** Focused on the deterministic nature of the anomaly scoring and the "zero-ML" edge processing angle.
- **Pathway Community Discord / Slack:** Sharing the project directly with the framework maintainers to show a robust, real-world application of their tool.

## 2. Launch Content Draft (Reddit / Hacker News)

**Title:** [Show HN/Reddit] ShieldAI - A deterministic, zero-ML industrial anomaly detection engine using Python & Pathway

**Body Draft:**
Hey everyone,

I wanted to share an open-source project I’ve been building called **ShieldAI**. It’s an Industrial Discharge Anomaly Detection System aimed at catching illegal / toxic wastewater dumping in real-time.

A lot of solutions in this space either rely on rigid, static SCADA thresholds (which cause alarm fatigue during normal shifts) or opaque Machine Learning models that are hard to explain to regulators. Instead, I built a deterministic, streaming engine. 

**How it works:**
* It reads high-volume, real-time IoT sensor data (COD, BOD, pH, TSS).
* It computes dynamic, rolling window statistics (mean, variance) using the **Pathway** streaming framework.
* It uses a Multivariate Root Mean Square z-scoring algorithm to find *coordinated* anomalies across multiple sensors (e.g., pH and COD both shifting simultaneously, even if neither breaks a hard limit).
* It scales mathematical anomalies against an Environmental Risk Index (ERI) to prioritize alerts based on the ecological sensitivity of the discharge location.

**Stack:** Python 3.12, Pathway, Pandas, Pytest (Fully typed, 90%+ coverage, strict flake8/mypy compliant).

I’d love for the data engineering and Python communities to tear it apart, look at the architecture, and give me feedback on the streaming join logic!

**Repo:** [Insert GitHub Link]
**Architecture Docs:** [Insert Link to docs/architecture.md]

## 3. Post-Launch Action Items
- Monitor GitHub Issues for initial feedback.
- Set up a Discord server for community contributions if traction exceeds 50+ stars.
- Write a 'Lessons Learned' Medium / Dev.to article focusing on the migration from static thresholds to streaming rolling windows.
