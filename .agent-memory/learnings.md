# Learnings

<!--
Record DISCOVERIES here - things that surprised you.
Format: brief, actionable insights (not session logs).
-->

- Actual JSON schema in school_threats_2026.json differs from PRD — has 23 fields including offense_date, time, who_threatened, lockdown_type, gender, custody, bond, additional_sources (not the normalized fields described in PRD)
- Dataset has 520 records in school_threats_2026.json (not 897 — that was the 2025 dataset)
- Frontend fetches from data/school_threats_2026.json at index.html:279
- rapidfuzz's partial_ratio handles "Tucker HS" vs "Tucker High School" perfectly (1.0 after normalization)
- Composite weighted scoring with 0.85 high threshold produces 0% false positives on real data

