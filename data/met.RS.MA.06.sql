-- =========================================================================
-- Metric: met.rs.ma.06
-- Description: Monthly incident count (running)
-- Logic: COUNT(DISTINCT NUMBER_VALUE)
-- Time Window: Current month based on OPENED_AT_VALUE
-- Source: m_servicenow_msi
-- Notes: Running count resets each month; no severity filter applied
-- =========================================================================
   MERGE INTO ${database}.${schema}.metric_daily_snapshots AS tgt
   USING (
     SELECT
        'met.rs.ma.06' AS metric_id,
        CURRENT_DATE AS dt,
        COUNT(DISTINCT NUMBER_VALUE) AS value,
        NULL AS target_value,
        NULL AS rag_status,
        'SUCCESS' AS collection_status,
        NULL AS task_run_id,
        'metric=incident_count; period=current_month; aggregation=distinct_number' AS notes

    FROM ${database}.${schema}.m_servicenow_msi
    WHERE OPENED_AT_VALUE >= DATE_TRUNC('month', CURRENT_DATE)
      AND OPENED_AT_VALUE <= CURRENT_DATE

) AS src

ON tgt.metric_id = src.metric_id
AND tgt.dt = src.dt

WHEN MATCHED
AND src.value IS NOT NULL
AND src.value > 0 THEN UPDATE SET
    tgt.value = src.value,
    tgt.target_value = src.target_value,
    tgt.rag_status = src.rag_status,
    tgt.collection_status = src.collection_status,
    tgt.notes = src.notes,
    tgt.updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    metric_id,
    dt,
    value,
    target_value,
    rag_status,
    collection_status,
    task_run_id,
    notes,
    inserted_at,
    updated_at
)
VALUES (
    src.metric_id,
    src.dt,
    src.value,
    src.target_value,
    src.rag_status,
    src.collection_status,
    src.task_run_id,
    src.notes,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);
