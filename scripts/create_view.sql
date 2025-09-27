CREATE OR REPLACE VIEW connection_details AS (
    SELECT DISTINCT
        elem ->> 'connect_name' AS connection_name,
        elem ->> 'scope' AS scope_name,
        elem ->> 'subscription_id' AS subscription_id,
        elem ->> 'management_group_id' AS management_group_id,
        elem ->> 'account_id' AS account_id,
        elem ->> 'status' AS connection_status,
        -- Generate standardized scope_id based on connection scope type
        CASE
            WHEN elem ->> 'scope' = 'SUBS' THEN LOWER(elem ->> 'subscription_id')
            WHEN elem ->> 'scope' = 'MG' THEN LOWER(
                elem ->> 'management_group_id'
            )
            WHEN elem ->> 'scope' = 'TENANT' THEN LOWER(
                elem ->> 'management_group_id'
            )
            WHEN elem ->> 'scope' = 'AWS_ORGANIZATION' THEN LOWER(elem ->> 'account_id')
            WHEN elem ->> 'scope' = 'AWS_ACCOUNT' THEN LOWER(elem ->> 'account_id')
        END AS scope_id
    FROM public.config, jsonb_array_elements(data) AS elem
    WHERE
        elem ->> 'connect_name' IS NOT NULL
);

CREATE OR REPLACE VIEW active_framework_connections AS (
    SELECT DISTINCT
        cd.connection_name,
        cd.scope_id,
        fcd.framework_id,
        fcd.policy_set_definition_id,
        CASE fcd.framework_id
            WHEN 'scp_aws' THEN 'UAE Cloud Sovereign Policies for AWS'
            ELSE fcd.csp_json ->> 'displayName'
        END AS framework_name,
        CASE fcd.framework_id
            WHEN 'scp' THEN 1
            WHEN 'acc' THEN 2
            WHEN 'sec' THEN 3
            WHEN 'scp_aws' THEN 4
            WHEN 'fsi' THEN 5
            WHEN 'cis' THEN 6
            WHEN 'iso27001' THEN 7
            ELSE NULL
        END AS framework_order
    FROM connection_details cd
        INNER JOIN public.framework_compliance_data fcd ON cd.scope_id = fcd.scope_id
    WHERE
        fcd.is_loaded = true
);

CREATE OR REPLACE VIEW framework_policy_details AS (
    SELECT
        DATE (date) AS policy_count_date,
        count::integer AS policy_count,
        scope_id,
        framework_id,
        loaded_version,
        policy_set_definition_id,
        csp_json ->> 'displayName' as framework_name
    FROM public.framework_compliance_data, LATERAL jsonb_each_text(
            json -> 'policy_count_history'
        ) AS kv (date, count)
    WHERE
        is_loaded = true
);

CREATE OR REPLACE VIEW daily_policy_compliance AS (
    SELECT
        policy_set_definition_id,
        framework_id,
        DATE (report_date) AS created_at,
        policy_definition_id,
        CASE
            WHEN COUNT(
                CASE
                    WHEN compliance_state = 'NonCompliant' THEN 1
                END
            ) > 0 THEN 'NonCompliant'
            ELSE 'Compliant'
        END AS control_assignment_compliance_status
    FROM public.mv_policy_compliance
    WHERE
        report_date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY
        policy_set_definition_id,
        framework_id,
        DATE (report_date),
        policy_definition_id
);

CREATE OR REPLACE VIEW daily_compliance_counts AS (
    SELECT
        policy_set_definition_id,
        framework_id,
        created_at,
        COUNT(
            control_assignment_compliance_status
        ) FILTER (
            WHERE
                control_assignment_compliance_status = 'NonCompliant'
        ) AS daily_total_non_compliant_count
    FROM daily_policy_compliance
    GROUP BY
        policy_set_definition_id,
        framework_id,
        created_at
);

CREATE OR REPLACE VIEW policy_changes_for_this_week AS (
    SELECT
        connection_name,
        framework_name,
        scope_id,
        created_at,
        loaded_version,
        daily_total_non_compliant_count,
        daily_total_compliant_count,
        daily_total_count,
        compliance_percentage
    FROM (
            select *, rank() over (
                    partition by
                        policy_set_definition_id, created_at
                    order by policy_count_date desc
                ) as rnk
            FROM (
                    SELECT DISTINCT
                        cd.connection_name AS connection_name, dcc.policy_set_definition_id AS policy_set_definition_id, afc.framework_name AS framework_name, afc.framework_order AS framework_order, cd.scope_id, dcc.created_at, fpc.policy_count_date, fpc.loaded_version, dcc.daily_total_non_compliant_count, fpc.policy_count - dcc.daily_total_non_compliant_count AS daily_total_compliant_count, fpc.policy_count as daily_total_count, ROUND(
                            COALESCE(
                                (
                                    (
                                        fpc.policy_count - dcc.daily_total_non_compliant_count
                                    ) * 100.0 / NULLIF(fpc.policy_count, 0)
                                ), 0
                            ), 2
                        ) AS compliance_percentage
                    FROM
                        daily_compliance_counts dcc
                        JOIN framework_policy_details fpc ON fpc.policy_set_definition_id = dcc.policy_set_definition_id
                        AND fpc.policy_count_date <= dcc.created_at
                        JOIN active_framework_connections afc ON afc.policy_set_definition_id = dcc.policy_set_definition_id
                        JOIN connection_details cd ON cd.scope_id = afc.scope_id
                    ORDER BY connection_name, afc.framework_order ASC, dcc.created_at DESC
                )
            ORDER BY
                connection_name, framework_order ASC, created_at DESC
        )
    WHERE
        rnk = 1
);