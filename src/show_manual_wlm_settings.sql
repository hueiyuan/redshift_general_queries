/**********************************************************************************************
Show manual wlm settings.

Reference: https://aws.amazon.com/tw/premiumsupport/knowledge-center/redshift-wlm-memory-allocation/


**********************************************************************************************/

SELECT rtrim(name) AS name,
        num_query_tasks AS slots,
        query_working_mem AS mem,
        max_execution_time AS max_time,
        user_group_wild_card AS user_wildcard,
        query_group_wild_card AS query_wildcard
FROM stv_wlm_service_class_config
WHERE service_class > 4;
