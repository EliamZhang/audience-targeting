-- ============================================================
-- 人群包取数 SQL
-- 四个包相互独立，各自单独执行。
--
-- 方言假设：CURRENT_DATE、INTERVAL '3' MONTH、ROW_NUMBER()
-- 按 Spark SQL / Trino / Presto 语法书写。
-- 若目标引擎不同请调整（例如 MySQL 的 INTERVAL 写法是 INTERVAL 3 MONTH）。
--
-- 主表字段已精简为进件 + 评级相关字段（见 ba.customer_profile_rawdata 的 base CTE），
-- 需要增删字段时四个包同步修改。
-- ============================================================


-- ============================================================
-- 包一：新老客风险分高评级表（筛选排除）
-- 顺序：风险分表先去重 → 再关联主表 → 最后筛选风险分
-- 路由：user_tag = 'New' 判新客分，user_tag = 'Existing' 判老客分
-- ============================================================
WITH new_risk AS (
    SELECT
        application_id,
        aus_new_risk_bid_3rdmodel_v1_0_20251201,
        ROW_NUMBER() OVER (PARTITION BY application_id ORDER BY inserttime DESC) AS rn
    FROM model.tmp_eliam_risk_000006_vintage_monitor
),
new_risk_dedup AS (
    SELECT
        application_id,
        aus_new_risk_bid_3rdmodel_v1_0_20251201
    FROM new_risk
    WHERE rn = 1
),
old_risk AS (
    SELECT
        application_id,
        old_risk_bid_mltmodel_v1_1_20251021,
        ROW_NUMBER() OVER (PARTITION BY application_id ORDER BY inserttime DESC) AS rn
    FROM model.tmp_eliam_risk_000004_strategy_run_log
),
old_risk_dedup AS (
    SELECT
        application_id,
        old_risk_bid_mltmodel_v1_1_20251021
    FROM old_risk
    WHERE rn = 1
),
base AS (
    SELECT
        user_id,
        application_id,
        user_tag,
        application_tag,
        application_time,
        application_date,
        status,
        application_status,
        last_step,
        completed_step,
        completed_step_time,
        risk_approved_time,
        risk_declined_time,
        converted_time,
        withdrawn_time,
        requested_loan_amount,
        requested_loan_tag,
        final_probability,
        risk_level,
        base_model_lightgbm_v2_prob,
        finv_predicted_probability_of_default,
        finv_risk_level,
        finv_color_code,
        finv_risk_tier,
        LTI,
        PTI,
        approval_rules_decision,
        combined_ml_models_decision,
        is_auto_approved,
        combined_reasons,
        total_income,
        total_expenses,
        gross_surplus,
        net_surplus,
        age,
        state,
        suburb,
        zip_code,
        dependents,
        is_bad_loan,
        duedate_3m_30,
        scheduled_cleared_date,
        final_closed_date,
        traffic_source,
        app_platform
    FROM ba.customer_profile_rawdata
    WHERE application_time >= '2024-01-01'
      -- 违约标记：只踢等于 1 的，NULL 与其他取值一律保留
      AND (duedate_3m_30 IS NULL OR duedate_3m_30 <> 1)
)
SELECT
    b.*,
    o.old_risk_bid_mltmodel_v1_1_20251021,
    n.aus_new_risk_bid_3rdmodel_v1_0_20251201
FROM base b
LEFT JOIN new_risk_dedup n
       ON b.application_id = n.application_id
      AND b.user_tag = 'New'
LEFT JOIN old_risk_dedup o
       ON b.application_id = o.application_id
      AND b.user_tag = 'Existing'
WHERE (b.user_tag = 'New' AND n.aus_new_risk_bid_3rdmodel_v1_0_20251201 BETWEEN 0 AND 0.075629)
   OR (b.user_tag = 'Existing' AND o.old_risk_bid_mltmodel_v1_1_20251021 BETWEEN 0 AND 0.05146)
;


-- ============================================================
-- 包二：新老客在贷表
-- 先在子查询里聚合到 application_id 粒度，再关联主表（避免一比多膨胀）
-- 基准日 CURRENT_DATE，取尚未到期且距今不足 3 个月的
-- ============================================================
WITH schedule_agg AS (
    SELECT
        application_id,
        MAX(scheduled_date) AS scheduled_date_max
    FROM ods.vintage_analysis_original_schedule_recoveries_v2
    GROUP BY application_id
),
base AS (
    SELECT
        user_id,
        application_id,
        user_tag,
        application_tag,
        application_time,
        application_date,
        status,
        application_status,
        last_step,
        completed_step,
        completed_step_time,
        risk_approved_time,
        risk_declined_time,
        converted_time,
        withdrawn_time,
        requested_loan_amount,
        requested_loan_tag,
        final_probability,
        risk_level,
        base_model_lightgbm_v2_prob,
        finv_predicted_probability_of_default,
        finv_risk_level,
        finv_color_code,
        finv_risk_tier,
        LTI,
        PTI,
        approval_rules_decision,
        combined_ml_models_decision,
        is_auto_approved,
        combined_reasons,
        total_income,
        total_expenses,
        gross_surplus,
        net_surplus,
        age,
        state,
        suburb,
        zip_code,
        dependents,
        is_bad_loan,
        duedate_3m_30,
        scheduled_cleared_date,
        final_closed_date,
        traffic_source,
        app_platform
    FROM ba.customer_profile_rawdata
)
SELECT
    b.*,
    s.scheduled_date_max
FROM base b
JOIN schedule_agg s
  ON b.application_id = s.application_id
WHERE b.status = 'Active_Account'
  AND s.scheduled_date_max >= CURRENT_DATE
  AND s.scheduled_date_max < CURRENT_DATE + INTERVAL '3' MONTH
;


-- ============================================================
-- 包三：新老客注册未完成申请
-- 口径：每个 user_id 只看最近一笔申请（最大 application_id）
-- ============================================================
WITH latest_application AS (
    SELECT
        user_id,
        MAX(application_id) AS latest_application_id
    FROM ba.customer_profile_rawdata
    GROUP BY user_id
),
base AS (
    SELECT
        user_id,
        application_id,
        user_tag,
        application_tag,
        application_time,
        application_date,
        status,
        application_status,
        last_step,
        completed_step,
        completed_step_time,
        risk_approved_time,
        risk_declined_time,
        converted_time,
        withdrawn_time,
        requested_loan_amount,
        requested_loan_tag,
        final_probability,
        risk_level,
        base_model_lightgbm_v2_prob,
        finv_predicted_probability_of_default,
        finv_risk_level,
        finv_color_code,
        finv_risk_tier,
        LTI,
        PTI,
        approval_rules_decision,
        combined_ml_models_decision,
        is_auto_approved,
        combined_reasons,
        total_income,
        total_expenses,
        gross_surplus,
        net_surplus,
        age,
        state,
        suburb,
        zip_code,
        dependents,
        is_bad_loan,
        duedate_3m_30,
        scheduled_cleared_date,
        final_closed_date,
        traffic_source,
        app_platform
    FROM ba.customer_profile_rawdata
)
SELECT
    b.*,
    CASE
        WHEN b.application_status IN ('0.Incomplete', '1.In Progress') THEN 1
        ELSE 0
    END AS incomplete_application
FROM base b
JOIN latest_application l
  ON b.user_id = l.user_id
 AND b.application_id = l.latest_application_id
WHERE b.application_status IN ('0.Incomplete', '1.In Progress')
;


-- ============================================================
-- 包四：新老客结清未复贷老户
-- 依据：application_id 越大表示时间越靠后
-- 输出：各用户最后一次结清的那笔申请
-- ============================================================
WITH last_closed AS (
    SELECT
        user_id,
        MAX(application_id) AS last_closed_id
    FROM ba.customer_profile_rawdata
    WHERE status = 'Closed'
    GROUP BY user_id
),
base AS (
    SELECT
        user_id,
        application_id,
        user_tag,
        application_tag,
        application_time,
        application_date,
        status,
        application_status,
        last_step,
        completed_step,
        completed_step_time,
        risk_approved_time,
        risk_declined_time,
        converted_time,
        withdrawn_time,
        requested_loan_amount,
        requested_loan_tag,
        final_probability,
        risk_level,
        base_model_lightgbm_v2_prob,
        finv_predicted_probability_of_default,
        finv_risk_level,
        finv_color_code,
        finv_risk_tier,
        LTI,
        PTI,
        approval_rules_decision,
        combined_ml_models_decision,
        is_auto_approved,
        combined_reasons,
        total_income,
        total_expenses,
        gross_surplus,
        net_surplus,
        age,
        state,
        suburb,
        zip_code,
        dependents,
        is_bad_loan,
        duedate_3m_30,
        scheduled_cleared_date,
        final_closed_date,
        traffic_source,
        app_platform
    FROM ba.customer_profile_rawdata
)
SELECT
    b.*
FROM base b
JOIN last_closed c
  ON b.user_id = c.user_id
 AND b.application_id = c.last_closed_id
WHERE NOT EXISTS (
    SELECT 1
    FROM ba.customer_profile_rawdata a
    WHERE a.user_id = b.user_id
      AND a.application_id > c.last_closed_id
      AND a.status = 'Active_Account'
)
;
