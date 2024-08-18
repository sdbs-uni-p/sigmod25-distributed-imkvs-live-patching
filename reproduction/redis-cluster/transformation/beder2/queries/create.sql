CREATE TABLE IF NOT EXISTS run (
    run_id VARCHAR PRIMARY KEY,
    run_commit STRING,
    run_success BOOLEAN,
    run_benchmark_failure BOOLEAN,
    run_benchmark_timeout BOOLEAN,
    run_db_failure BOOLEAN,
    run_name STRING,

    -- patch generation
    patch_generation_patches INTEGER,
    patch_generation_patch_paths STRING,

    -- data
    data_requests INTEGER,
    data_keyspace HUGEINT,
    data_data_size INTEGER,
    data_max_memory_usage_gb INTEGER,
    
    -- benchmark
    benchmark_taskset_start INTEGER,
    benchmark_taskset_end INTEGER,
    benchmark_taskset_step INTEGER,
    benchmark_name STRING,
    benchmark_output_name STRING,
    benchmark_requests INTEGER,
    benchmark_time_s INTEGER,
    benchmark_idle_before INTEGER,
    benchmark_keyspace HUGEINT,
    benchmark_data_size INTEGER,
    benchmark_clients INTEGER,
    benchmark_threads INTEGER,
    benchmark_incr_key_only BOOLEAN,
    benchmark_dump_db_at_stop BOOLEAN,
    benchmark_max_memory_usage_gb INTEGER,
    benchmark_framework_start_time DOUBLE,
    benchmark_framework_end_time DOUBLE,
    benchmark_framework_end_time_s DOUBLE,

    -- failover
    failover_failover_after_s INTEGER,
    failover_skip_failover BOOLEAN,
    failover_skip_restart_master BOOLEAN,
    failover_skip_restart_replica BOOLEAN,

    -- redis-cluster
    redis_cluster_config STRING,
    redis_cluster_status_every_s INTEGER,


    -- patch
    patch_method STRING,
    patch_distribution STRING,
    patch_reverse_version BOOLEAN,
    patch_apply_patch_after_s INTEGER,
    patch_measure_vma BOOLEAN,
    patch_measure_pte BOOLEAN,
    patch_patch_only_active BOOLEAN,
    patch_apply_all_patches_at_once BOOLEAN,
    patch_single_as_for_each_thread BOOLEAN,
    patch_skip_patch_file BOOLEAN,
);

CREATE TABLE IF NOT EXISTS latencies_info (
    start_time DOUBLE,
    end_time DOUBLE,
    total_duration_s DOUBLE,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);


CREATE TABLE IF NOT EXISTS latencies (
    thread_id INTEGER,
    latency_ms DOUBLE,
    time DOUBLE,
    port INTEGER,
    client_id INTEGER,
    key STRING,
    time_s DOUBLE,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS cluster_status (
    time DOUBLE,
    time_s DOUBLE,
    port INTEGER,
    master_port INTEGER,
    role STRING,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);



CREATE TABLE IF NOT EXISTS wf_r_new_patch (
    port INTEGER,
    time DOUBLE,
    time_s DOUBLE,
    name STRING,
    version INTEGER,
    method INTEGER,
    from_client BOOLEAN,
    entry_counter INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS wf_r_patch_sent (
    port INTEGER,
    time DOUBLE,
    time_s DOUBLE,
    name STRING,
    receiver STRING,
    version INTEGER,
    entry_counter INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS wf_r_patch_request (
    port INTEGER,
    time DOUBLE,
    time_s DOUBLE,
    name STRING,
    receiver STRING,
    version INTEGER,
    entry_counter INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS wf_r_patch_applied (
    port INTEGER,
    time DOUBLE,
    time_s DOUBLE,
    name STRING,
    version INTEGER,
    method INTEGER,
    entry_counter INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS wf_r_patch_signaled (
    port INTEGER,
    time DOUBLE,
    time_s DOUBLE,
    name STRING,
    version INTEGER,
    method INTEGER,
    entry_counter INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);


CREATE TABLE IF NOT EXISTS wf_r_patch_received (
    port INTEGER,
    time DOUBLE,
    time_s DOUBLE,
    name STRING,
    sender STRING,
    version INTEGER,
    method INTEGER,
    entry_counter INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS wf_l_birth (
    port INTEGER,
    name VARCHAR,
    time DOUBLE,
    time_s DOUBLE,
    thread_name VARCHAR,
    thread_group_name VARCHAR,
    thread_id INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS wf_l_death (
    port INTEGER,
    name VARCHAR,
    duration_ms DOUBLE,
    thread_name VARCHAR,
    thread_group_name VARCHAR,
    thread_id INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS wf_l_apply (
    port INTEGER,
    name VARCHAR,
    time DOUBLE,
    time_s DOUBLE,
    quiescence VARCHAR,
    amount_threads INTEGER,
    thread_group_name VARCHAR,
    entry_counter INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS wf_l_finished (
    port INTEGER,
    name VARCHAR,
    duration_ms DOUBLE,
    thread_group_name VARCHAR,
    entry_counter INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS wf_l_migrated (
    port INTEGER,
    name VARCHAR,
    duration_ms DOUBLE,
    current_address_space_version INTEGER,
    thread_name VARCHAR,
    thread_group_name VARCHAR,
    thread_id INTEGER,
    entry_counter INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS wf_l_quiescence (
    port INTEGER,
    name VARCHAR,
    duration_ms DOUBLE,
    thread_group_name VARCHAR,
    entry_counter INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);


CREATE TABLE IF NOT EXISTS wf_l_reach_quiescence (
    port INTEGER,
    name VARCHAR,
    duration_ms DOUBLE,
    thread_name VARCHAR,
    thread_group_name VARCHAR,
    thread_id INTEGER,
    entry_counter INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS wf_l_as_new (
    port INTEGER,
    name VARCHAR,
    duration_ms DOUBLE,
    thread_group_name VARCHAR,
    entry_counter INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS wf_l_as_delete (
    port INTEGER,
    name VARCHAR,
    duration_ms DOUBLE,
    thread_group_name VARCHAR,
    entry_counter INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS wf_l_as_switch (
    port INTEGER,
    name VARCHAR,
    duration_ms DOUBLE,
    entry_counter INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS wf_l_patched (
    port INTEGER,
    name VARCHAR,
    duration_ms DOUBLE,
    entry_counter INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS wf_l_e2e_patched (
    port INTEGER,
    name VARCHAR,
    time DOUBLE,
    time_s DOUBLE,
    duration_ms DOUBLE,
    patch_method INTEGER,
    thread_group_name VARCHAR,
    entry_counter INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS wf_l_pte (
    port INTEGER,
    name VARCHAR,
    size_kB INTEGER,
    entry_counter INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);


CREATE TABLE IF NOT EXISTS wf_l_vma (
    port INTEGER,
    name VARCHAR,
    wfpatch_generation INTEGER,
    perm VARCHAR,
    type VARCHAR,
    count INTEGER,
    size INTEGER,
    shared_clean INTEGER,
    shared_dirty INTEGER,
    private_clean INTEGER,
    private_dirty INTEGER,
    entry_counter INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS patch_file (
    name VARCHAR,
    size_bytes INTEGER,
    -- References
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS patch_elf (
    file_name VARCHAR,
    section_name VARCHAR,
    size_bytes INTEGER,
    type VARCHAR,
    -- Reference
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS failover (
    -- common entries
    log_time DOUBLE,
    log_time_s DOUBLE,
    start_time DOUBLE,
    start_time_s DOUBLE,
    end_time DOUBLE,
    end_time_s DOUBLE,
    port INTEGER,
    name STRING,
    duration_ms DOUBLE,
    
    -- specific entries
    action STRING,
    action_duration_ms DOUBLE,
    action_time DOUBLE,
    action_time_s DOUBLE,
    
    -- Reference
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS rdb (
    port INTEGER,
    key STRING,

    -- Reference
    run_id VARCHAR -- REFERENCES run(run_id)

);
CREATE TABLE IF NOT EXISTS redis_all_network (
    bytes BIGINT,
    time DOUBLE,
    time_s DOUBLE,
    port INTEGER,
    -- Reference
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS redis_io_write (
    bytes BIGINT,
    time DOUBLE,
    time_s DOUBLE,
    port INTEGER,
    -- Reference
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS redis_io_read (
    bytes BIGINT,
    time DOUBLE,
    time_s DOUBLE,
    port INTEGER,
    -- Reference
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS redis_cluster_network (
    bytes BIGINT,
    time DOUBLE,
    time_s DOUBLE,
    port INTEGER,
    -- Reference
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS redis_network_summary (
    cluster_packets_processed BIGINT,
    port INTEGER,
    -- Reference
    run_id VARCHAR -- REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS redis_bgsaves (
    time DOUBLE,
    time_s DOUBLE,
    port INTEGER,
    run_id VARCHAR --REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS redis_restart (
    start_time DOUBLE,
    start_time_s DOUBLE,
    end_time DOUBLE,
    end_time_s DOUBLE,
    duration_s DOUBLE,
    port INTEGER,
    run_id VARCHAR --REFERENCES run(run_id)
);

CREATE TABLE IF NOT EXISTS redis_failover (
    start_time DOUBLE,
    start_time_s DOUBLE,
    end_time DOUBLE,
    end_time_s DOUBLE,
    duration_ms DOUBLE,
    port INTEGER,
    run_id VARCHAR --REFERENCES run(run_id)
);

-- CREATE TABLE IF NOT EXISTS redis_network (
--     bytes BIGINT,
--     time DOUBLE,
--     entry INTEGER,
--     time_s DOUBLE,
--     port INTEGER,
-- 
--     -- Reference
--     run_id VARCHAR -- REFERENCES run(run_id)
-- 
-- );

