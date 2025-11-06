# BI Portfolio — Roman Kabanov

End-to-end analytics platform for real estate agency:
- **Data sources**: CRM API, advertising systems
- **Storage**: PostgreSQL (staging) → ClickHouse (DWH)
- **BI**: Yandex DataLens with Row-Level Security
- **ETL**: Airbyte + custom Python scripts
- **Key features**: 
  - Drill-down from KPI to deal level
  - Incremental data sync every 4 hours
  - Full Git versioning of SQL and ETL logic

▶ Live dashboard: https://datalens.ru/gallery/t90fp8ur9vsqf?preview=1
