CREATE OR REPLACE VIEW connection_details AS (
    SELECT DISTINCT
        elem ->> 'connect_name' AS connection_name,
        elem ->> 'scope' AS scope_name,
        elem ->> 'subscription_id' AS subscription_id,
        elem ->> 'management_group_id' AS management_group_id,
        elem ->> 'account_id' AS account_id,
        elem ->> 'status' AS status,
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