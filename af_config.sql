create schema if not exists main;

SET search_path = main;

create sequence task_id_sequence;

create sequence taskset_id_sequence;

create table ab_permission
(
    id serial
        primary key,
    name varchar(100) not null
        unique
        constraint ab_permission_name_uq
            unique
);

create table ab_register_user
(
    id serial
        primary key,
    first_name varchar(256) not null,
    last_name varchar(256) not null,
    username varchar(512) not null
        unique
        constraint ab_register_user_username_uq
            unique,
    password varchar(256),
    email varchar(512) not null,
    registration_date timestamp,
    registration_hash varchar(256)
);

create unique index idx_ab_register_user_username
    on ab_register_user (lower(username::text));

create table ab_role
(
    id serial
        primary key,
    name varchar(64) not null
        unique
        constraint ab_role_name_uq
            unique
);

create table ab_user
(
    id serial
        primary key,
    first_name varchar(256) not null,
    last_name varchar(256) not null,
    username varchar(512) not null
        unique
        constraint ab_user_username_uq
            unique,
    password varchar(256),
    active boolean,
    email varchar(512) not null
        unique
        constraint ab_user_email_uq
            unique,
    last_login timestamp,
    login_count integer,
    fail_login_count integer,
    created_on timestamp,
    changed_on timestamp,
    created_by_fk integer
        references ab_user,
    changed_by_fk integer
        references ab_user
);

create unique index idx_ab_user_username
    on ab_user (lower(username::text));

create table ab_user_role
(
    id serial
        primary key,
    user_id integer
        references ab_user,
    role_id integer
        references ab_role,
    unique (user_id, role_id),
    constraint ab_user_role_user_id_role_id_uq
        unique (user_id, role_id)
);

create table ab_view_menu
(
    id serial
        primary key,
    name varchar(250) not null
        unique
        constraint ab_view_menu_name_uq
            unique
);

create table ab_permission_view
(
    id serial
        primary key,
    permission_id integer
        references ab_permission,
    view_menu_id integer
        references ab_view_menu,
    unique (permission_id, view_menu_id),
    constraint ab_permission_view_permission_id_view_menu_id_uq
        unique (permission_id, view_menu_id)
);

create table ab_permission_view_role
(
    id serial
        primary key,
    permission_view_id integer
        references ab_permission_view,
    role_id integer
        references ab_role,
    unique (permission_view_id, role_id),
    constraint ab_permission_view_role_permission_view_id_role_id_uq
        unique (permission_view_id, role_id)
);

create table alembic_version
(
    version_num varchar(32) not null
        constraint alembic_version_pkc
            primary key
);

create table callback_request
(
    id serial
        primary key,
    created_at timestamp with time zone not null,
    priority_weight integer not null,
    callback_data json not null,
    callback_type varchar(20) not null,
    processor_subdir varchar(2000)
);

create table celery_taskmeta
(
    id integer not null
        primary key,
    task_id varchar(155)
        unique,
    status varchar(50),
    result bytea,
    date_done timestamp,
    traceback text,
    name varchar(155),
    args bytea,
    kwargs bytea,
    worker varchar(155),
    retries integer,
    queue varchar(155)
);

create table celery_tasksetmeta
(
    id integer not null
        primary key,
    taskset_id varchar(155)
        unique,
    result bytea,
    date_done timestamp
);

create table connection
(
    id serial
        primary key,
    conn_id varchar(250) not null
        constraint connection_conn_id_uq
            unique
        constraint unique_conn_id
            unique,
    conn_type varchar(500) not null,
    host varchar(500),
    schema varchar(500),
    login text,
    password text,
    port integer,
    extra text,
    is_encrypted boolean,
    is_extra_encrypted boolean,
    description text
);

create table dag
(
    dag_id varchar(250) not null
        primary key,
    is_paused boolean,
    is_subdag boolean,
    is_active boolean,
    last_parsed_time timestamp with time zone,
    last_pickled timestamp with time zone,
    last_expired timestamp with time zone,
    scheduler_lock boolean,
    pickle_id integer,
    fileloc varchar(2000),
    owners varchar(2000),
    description text,
    default_view varchar(25),
    schedule_interval text,
    root_dag_id varchar(250),
    next_dagrun timestamp with time zone,
    next_dagrun_create_after timestamp with time zone,
    max_active_tasks integer not null,
    has_task_concurrency_limits boolean not null,
    max_active_runs integer,
    next_dagrun_data_interval_start timestamp with time zone,
    next_dagrun_data_interval_end timestamp with time zone,
    has_import_errors boolean default false,
    processor_subdir varchar(2000),
    timetable_description varchar(1000),
    dag_display_name varchar(2000),
    dataset_expression json,
    max_consecutive_failed_dag_runs integer not null
);

create index idx_next_dagrun_create_after
    on dag (next_dagrun_create_after);

create index idx_root_dag_id
    on dag (root_dag_id);

create table dag_code
(
    fileloc_hash bigint not null
        primary key,
    fileloc varchar(2000) not null,
    source_code text not null,
    last_updated timestamp with time zone not null
);

create table dag_owner_attributes
(
    dag_id varchar(250) not null
        constraint "dag.dag_id"
            references dag
                on delete cascade,
    owner varchar(500) not null,
    link varchar(500) not null,
    primary key (dag_id, owner)
);

create table dag_pickle
(
    id serial
        primary key,
    pickle bytea,
    created_dttm timestamp with time zone,
    pickle_hash bigint
);

create table dag_tag
(
    name varchar(100) not null,
    dag_id varchar(250) not null
        references dag
            on delete cascade,
    primary key (name, dag_id)
);

create table dag_warning
(
    dag_id varchar(250) not null
        constraint dcw_dag_id_fkey
            references dag
                on delete cascade,
    warning_type varchar(50) not null,
    message text not null,
    timestamp timestamp with time zone not null,
    primary key (dag_id, warning_type)
);

create table dataset
(
    id serial
        primary key,
    uri varchar(3000) not null,
    extra json not null,
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone not null,
    is_orphaned boolean default false not null
);

create table dag_schedule_dataset_reference
(
    dataset_id integer not null
        constraint dsdr_dataset_fkey
            references dataset
                on delete cascade,
    dag_id varchar(250) not null
        constraint dsdr_dag_id_fkey
            references dag
                on delete cascade,
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone not null,
    constraint dsdr_pkey
        primary key (dataset_id, dag_id)
);

create unique index idx_uri_unique
    on dataset (uri);

create table dataset_dag_run_queue
(
    dataset_id integer not null
        constraint ddrq_dataset_fkey
            references dataset
                on delete cascade,
    target_dag_id varchar(250) not null
        constraint ddrq_dag_fkey
            references dag
                on delete cascade,
    created_at timestamp with time zone not null,
    constraint datasetdagrunqueue_pkey
        primary key (dataset_id, target_dag_id)
);

create table dataset_event
(
    id serial
        primary key,
    dataset_id integer not null,
    extra json not null,
    source_task_id varchar(250),
    source_dag_id varchar(250),
    source_run_id varchar(250),
    source_map_index integer default '-1'::integer,
    timestamp timestamp with time zone not null
);

create index idx_dataset_id_timestamp
    on dataset_event (dataset_id, timestamp);

create table import_error
(
    id serial
        primary key,
    timestamp timestamp with time zone,
    filename varchar(1024),
    stacktrace text,
    processor_subdir varchar(2000)
);

create table job
(
    id serial
        primary key,
    dag_id varchar(250),
    state varchar(20),
    job_type varchar(30),
    start_date timestamp with time zone,
    end_date timestamp with time zone,
    latest_heartbeat timestamp with time zone,
    executor_class varchar(500),
    hostname varchar(500),
    unixname varchar(1000)
);

create index idx_job_dag_id
    on job (dag_id);

create index idx_job_state_heartbeat
    on job (state, latest_heartbeat);

create index job_type_heart
    on job (job_type, latest_heartbeat);

create table log
(
    id serial
        primary key,
    dttm timestamp with time zone,
    dag_id varchar(250),
    task_id varchar(250),
    event varchar(60),
    execution_date timestamp with time zone,
    owner varchar(500),
    extra text,
    map_index integer,
    run_id varchar(250),
    owner_display_name varchar(500)
);

create index idx_log_dag
    on log (dag_id);

create index idx_log_dttm
    on log (dttm);

create index idx_log_event
    on log (event);

create table log_template
(
    id serial
        primary key,
    filename text not null,
    elasticsearch_id text not null,
    created_at timestamp with time zone not null
);

create table dag_run
(
    id serial
        primary key,
    dag_id varchar(250) not null,
    execution_date timestamp with time zone not null,
    state varchar(50),
    run_id varchar(250) not null,
    external_trigger boolean,
    conf bytea,
    end_date timestamp with time zone,
    start_date timestamp with time zone,
    run_type varchar(50) not null,
    last_scheduling_decision timestamp with time zone,
    dag_hash varchar(32),
    creating_job_id integer,
    queued_at timestamp with time zone,
    data_interval_start timestamp with time zone,
    data_interval_end timestamp with time zone,
    log_template_id integer
        constraint task_instance_log_template_id_fkey
            references log_template,
    updated_at timestamp with time zone,
    clear_number integer not null,
    unique (dag_id, execution_date),
    unique (dag_id, run_id)
);

create index dag_id_state
    on dag_run (dag_id, state);

create index idx_dag_run_dag_id
    on dag_run (dag_id);

create index idx_dag_run_queued_dags
    on dag_run (state, dag_id)
    where ((state)::text = 'queued'::text);

create index idx_dag_run_running_dags
    on dag_run (state, dag_id)
    where ((state)::text = 'running'::text);

create index idx_last_scheduling_decision
    on dag_run (last_scheduling_decision);

create table dag_run_note
(
    user_id integer
        constraint dag_run_note_user_fkey
            references ab_user,
    dag_run_id integer not null
        primary key
        constraint dag_run_note_dr_fkey
            references dag_run
                on delete cascade,
    content varchar(1000),
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone not null
);

create table dagrun_dataset_event
(
    dag_run_id integer not null
        references dag_run
            on delete cascade,
    event_id integer not null
        references dataset_event
            on delete cascade,
    primary key (dag_run_id, event_id)
);

create index idx_dagrun_dataset_events_dag_run_id
    on dagrun_dataset_event (dag_run_id);

create index idx_dagrun_dataset_events_event_id
    on dagrun_dataset_event (event_id);

create table serialized_dag
(
    dag_id varchar(250) not null
        primary key,
    fileloc varchar(2000) not null,
    fileloc_hash bigint not null,
    data json,
    last_updated timestamp with time zone not null,
    dag_hash varchar(32) not null,
    data_compressed bytea,
    processor_subdir varchar(2000)
);

create index idx_fileloc_hash
    on serialized_dag (fileloc_hash);

create table session
(
    id serial
        primary key,
    session_id varchar(255)
        unique,
    data bytea,
    expiry timestamp
);

create table sla_miss
(
    task_id varchar(250) not null,
    dag_id varchar(250) not null,
    execution_date timestamp with time zone not null,
    email_sent boolean,
    timestamp timestamp with time zone,
    description text,
    notification_sent boolean,
    primary key (task_id, dag_id, execution_date)
);

create index sm_dag
    on sla_miss (dag_id);

create table slot_pool
(
    id serial
        primary key,
    pool varchar(256)
        unique
        constraint slot_pool_pool_uq
            unique,
    slots integer,
    description text,
    include_deferred boolean default false not null
);

create table task_outlet_dataset_reference
(
    dataset_id integer not null
        constraint todr_dataset_fkey
            references dataset
                on delete cascade,
    dag_id varchar(250) not null
        constraint todr_dag_id_fkey
            references dag
                on delete cascade,
    task_id varchar(250) not null,
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone not null,
    constraint todr_pkey
        primary key (dataset_id, dag_id, task_id)
);

create table trigger
(
    id serial
        primary key,
    classpath varchar(1000) not null,
    kwargs text not null,
    created_date timestamp not null,
    triggerer_id integer
);

create table task_instance
(
    task_id varchar(250) not null,
    dag_id varchar(250) not null,
    run_id varchar(250) not null,
    start_date timestamp with time zone,
    end_date timestamp with time zone,
    duration double precision,
    state varchar(20),
    try_number integer,
    hostname varchar(1000),
    unixname varchar(1000),
    job_id integer,
    pool varchar(256) not null,
    queue varchar(256),
    priority_weight integer,
    operator varchar(1000),
    queued_dttm timestamp with time zone,
    pid integer,
    max_tries integer default '-1'::integer,
    executor_config bytea,
    pool_slots integer not null,
    queued_by_job_id integer,
    external_executor_id varchar(250),
    trigger_id integer
        references trigger
            on delete cascade,
    trigger_timeout timestamp,
    next_method varchar(1000),
    next_kwargs json,
    map_index integer default '-1'::integer not null,
    updated_at timestamp with time zone,
    custom_operator_name varchar(1000),
    rendered_map_index varchar(250),
    task_display_name varchar(2000),
    primary key (dag_id, task_id, run_id, map_index),
    constraint task_instance_dag_run_fkey
        foreign key (dag_id, run_id) references dag_run (dag_id, run_id)
            on delete cascade
);

create table rendered_task_instance_fields
(
    dag_id varchar(250) not null,
    task_id varchar(250) not null,
    rendered_fields json not null,
    k8s_pod_yaml json,
    run_id varchar(250) not null,
    map_index integer default '-1'::integer not null,
    primary key (dag_id, task_id, run_id, map_index),
    constraint rtif_ti_fkey
        foreign key (dag_id, task_id, run_id, map_index) references task_instance
            on delete cascade
);

create table task_fail
(
    id serial
        primary key,
    task_id varchar(250) not null,
    dag_id varchar(250) not null,
    start_date timestamp with time zone,
    end_date timestamp with time zone,
    duration integer,
    run_id varchar(250) not null,
    map_index integer default '-1'::integer not null,
    constraint task_fail_ti_fkey
        foreign key (dag_id, task_id, run_id, map_index) references task_instance
            on delete cascade
);

create index idx_task_fail_task_instance
    on task_fail (dag_id, task_id, run_id, map_index);

create index ti_dag_run
    on task_instance (dag_id, run_id);

create index ti_dag_state
    on task_instance (dag_id, state);

create index ti_job_id
    on task_instance (job_id);

create index ti_pool
    on task_instance (pool, state, priority_weight);

create index ti_state
    on task_instance (state);

create index ti_state_lkp
    on task_instance (dag_id, task_id, run_id, state);

create index ti_trigger_id
    on task_instance (trigger_id);

create table task_instance_note
(
    user_id integer
        constraint task_instance_note_user_fkey
            references ab_user,
    task_id varchar(250) not null,
    dag_id varchar(250) not null,
    run_id varchar(250) not null,
    map_index integer not null,
    content varchar(1000),
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone not null,
    primary key (task_id, dag_id, run_id, map_index),
    constraint task_instance_note_ti_fkey
        foreign key (dag_id, task_id, run_id, map_index) references task_instance
            on delete cascade
);

create table task_map
(
    dag_id varchar(250) not null,
    task_id varchar(250) not null,
    run_id varchar(250) not null,
    map_index integer not null,
    length integer not null
        constraint ck_task_map_task_map_length_not_negative
            check (length >= 0),
    keys json,
    primary key (dag_id, task_id, run_id, map_index),
    constraint task_map_task_instance_fkey
        foreign key (dag_id, task_id, run_id, map_index) references task_instance
            on update cascade on delete cascade
);

create table task_reschedule
(
    id serial
        primary key,
    task_id varchar(250) not null,
    dag_id varchar(250) not null,
    try_number integer not null,
    start_date timestamp with time zone not null,
    end_date timestamp with time zone not null,
    duration integer not null,
    reschedule_date timestamp with time zone not null,
    run_id varchar(250) not null,
    map_index integer default '-1'::integer not null,
    constraint task_reschedule_dr_fkey
        foreign key (dag_id, run_id) references dag_run (dag_id, run_id)
            on delete cascade,
    constraint task_reschedule_ti_fkey
        foreign key (dag_id, task_id, run_id, map_index) references task_instance
            on delete cascade
);

create index idx_task_reschedule_dag_run
    on task_reschedule (dag_id, run_id);

create index idx_task_reschedule_dag_task_run
    on task_reschedule (dag_id, task_id, run_id, map_index);

create table variable
(
    id serial
        primary key,
    key varchar(250)
        unique
        constraint variable_key_uq
            unique,
    val text,
    is_encrypted boolean,
    description text
);

create table xcom
(
    key varchar(512) not null,
    value bytea,
    timestamp timestamp with time zone not null,
    task_id varchar(250) not null,
    dag_id varchar(250) not null,
    dag_run_id integer not null,
    map_index integer default '-1'::integer not null,
    run_id varchar(250) not null,
    primary key (dag_run_id, task_id, map_index, key),
    constraint xcom_task_instance_fkey
        foreign key (dag_id, task_id, run_id, map_index) references task_instance
            on delete cascade
);

create index idx_xcom_key
    on xcom (key);

create index idx_xcom_task_instance
    on xcom (dag_id, task_id, run_id, map_index);


INSERT INTO
    main.slot_pool (pool, slots, description, include_deferred)
VALUES
    ('default_pool', 128, 'Default pool', FALSE)
;


INSERT INTO main.alembic_version(version_num) VALUES('1949afb29106');

-- Running upgrade 1949afb29106 -> bff083ad727d

DROP INDEX idx_last_scheduling_decision;

UPDATE alembic_version SET version_num='bff083ad727d' WHERE alembic_version.version_num = '1949afb29106';

-- Running upgrade bff083ad727d -> 686269002441

ALTER TABLE connection DROP CONSTRAINT IF EXISTS unique_conn_id;

ALTER TABLE connection DROP CONSTRAINT IF EXISTS connection_conn_id_uq;

ALTER TABLE connection ADD CONSTRAINT connection_conn_id_uq UNIQUE (conn_id);

UPDATE dag SET max_consecutive_failed_dag_runs='0' WHERE TRUE;

ALTER TABLE dag ALTER COLUMN max_consecutive_failed_dag_runs SET NOT NULL;

ALTER TABLE task_instance DROP CONSTRAINT task_instance_dag_run_fkey;

ALTER TABLE task_reschedule DROP CONSTRAINT task_reschedule_dr_fkey;

ALTER TABLE dag_run DROP CONSTRAINT IF EXISTS dag_run_dag_id_execution_date_uq;

ALTER TABLE dag_run DROP CONSTRAINT IF EXISTS dag_run_dag_id_run_id_uq;

ALTER TABLE dag_run DROP CONSTRAINT IF EXISTS dag_run_dag_id_execution_date_key;

ALTER TABLE dag_run DROP CONSTRAINT IF EXISTS dag_run_dag_id_run_id_key;

ALTER TABLE dag_run ADD CONSTRAINT dag_run_dag_id_execution_date_key UNIQUE (dag_id, execution_date);

ALTER TABLE dag_run ADD CONSTRAINT dag_run_dag_id_run_id_key UNIQUE (dag_id, run_id);

ALTER TABLE task_instance ADD CONSTRAINT task_instance_dag_run_fkey FOREIGN KEY(dag_id, run_id) REFERENCES dag_run (dag_id, run_id) ON DELETE CASCADE;

ALTER TABLE task_reschedule ADD CONSTRAINT task_reschedule_dr_fkey FOREIGN KEY(dag_id, run_id) REFERENCES dag_run (dag_id, run_id) ON DELETE CASCADE;

UPDATE alembic_version SET version_num='686269002441' WHERE alembic_version.version_num = 'bff083ad727d';

-- Running upgrade 686269002441 -> 0fd0c178cbe8

CREATE INDEX idx_dag_schedule_dataset_reference_dag_id ON dag_schedule_dataset_reference (dag_id);

CREATE INDEX idx_dag_tag_dag_id ON dag_tag (dag_id);

CREATE INDEX idx_dag_warning_dag_id ON dag_warning (dag_id);

CREATE INDEX idx_dataset_dag_run_queue_target_dag_id ON dataset_dag_run_queue (target_dag_id);

CREATE INDEX idx_task_outlet_dataset_reference_dag_id ON task_outlet_dataset_reference (dag_id);

UPDATE alembic_version SET version_num='0fd0c178cbe8' WHERE alembic_version.version_num = '686269002441';

-- Running upgrade 0fd0c178cbe8 -> 677fdbb7fc54

ALTER TABLE task_instance ADD COLUMN executor VARCHAR(1000);

UPDATE alembic_version SET version_num='677fdbb7fc54' WHERE alembic_version.version_num = '0fd0c178cbe8';

-- Running upgrade 677fdbb7fc54 -> c4602ba06b4b

CREATE TABLE dag_priority_parsing_request (
    id VARCHAR(32) NOT NULL,
    fileloc VARCHAR(2000) NOT NULL,
    CONSTRAINT dag_priority_parsing_request_pkey PRIMARY KEY (id)
);

UPDATE alembic_version SET version_num='c4602ba06b4b' WHERE alembic_version.version_num = '677fdbb7fc54';

-- Running upgrade c4602ba06b4b -> d482b7261ff9

CREATE TABLE task_instance_history (
    id SERIAL NOT NULL,
    task_id VARCHAR(250) NOT NULL,
    dag_id VARCHAR(250) NOT NULL,
    run_id VARCHAR(250) NOT NULL,
    map_index INTEGER DEFAULT -1 NOT NULL,
    try_number INTEGER NOT NULL,
    start_date TIMESTAMP WITH TIME ZONE,
    end_date TIMESTAMP WITH TIME ZONE,
    duration FLOAT,
    state VARCHAR(20),
    max_tries INTEGER DEFAULT -1,
    hostname VARCHAR(1000),
    unixname VARCHAR(1000),
    job_id INTEGER,
    pool VARCHAR(256) NOT NULL,
    pool_slots INTEGER NOT NULL,
    queue VARCHAR(256),
    priority_weight INTEGER,
    operator VARCHAR(1000),
    custom_operator_name VARCHAR(1000),
    queued_dttm TIMESTAMP WITH TIME ZONE,
    queued_by_job_id INTEGER,
    pid INTEGER,
    executor VARCHAR(1000),
    executor_config BYTEA,
    updated_at TIMESTAMP WITH TIME ZONE,
    rendered_map_index VARCHAR(250),
    external_executor_id VARCHAR(250),
    trigger_id INTEGER,
    trigger_timeout TIMESTAMP WITHOUT TIME ZONE,
    next_method VARCHAR(1000),
    next_kwargs JSON,
    task_display_name VARCHAR(2000),
    CONSTRAINT task_instance_history_pkey PRIMARY KEY (id),
    CONSTRAINT task_instance_history_ti_fkey FOREIGN KEY(dag_id, task_id, run_id, map_index) REFERENCES task_instance (dag_id, task_id, run_id, map_index) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT task_instance_history_dtrt_uq UNIQUE (dag_id, task_id, run_id, map_index, try_number)
);

UPDATE alembic_version SET version_num='d482b7261ff9' WHERE alembic_version.version_num = 'c4602ba06b4b';

-- Running upgrade d482b7261ff9 -> 05e19f3176be

CREATE TABLE dataset_alias (
    id SERIAL NOT NULL,
    name VARCHAR(3000) NOT NULL,
    CONSTRAINT dataset_alias_pkey PRIMARY KEY (id)
);

CREATE UNIQUE INDEX idx_name_unique ON dataset_alias (name);

UPDATE alembic_version SET version_num='05e19f3176be' WHERE alembic_version.version_num = 'd482b7261ff9';

-- Running upgrade 05e19f3176be -> ec3471c1e067

CREATE TABLE dataset_alias_dataset_event (
    alias_id INTEGER NOT NULL,
    event_id INTEGER NOT NULL,
    CONSTRAINT dataset_alias_dataset_event_pkey PRIMARY KEY (alias_id, event_id),
    CONSTRAINT dataset_alias_dataset_event_alias_id_fkey FOREIGN KEY(alias_id) REFERENCES dataset_alias (id) ON DELETE CASCADE,
    CONSTRAINT dataset_alias_dataset_event_event_id_fkey FOREIGN KEY(event_id) REFERENCES dataset_event (id) ON DELETE CASCADE
);

CREATE INDEX idx_dataset_alias_dataset_event_alias_id ON dataset_alias_dataset_event (alias_id);

CREATE INDEX idx_dataset_alias_dataset_event_event_id ON dataset_alias_dataset_event (event_id);

UPDATE alembic_version SET version_num='ec3471c1e067' WHERE alembic_version.version_num = '05e19f3176be';

-- Running upgrade ec3471c1e067 -> 41b3bc7c0272

ALTER TABLE log ADD COLUMN try_number INTEGER;

CREATE INDEX idx_log_task_instance ON log (dag_id, task_id, run_id, map_index, try_number);

UPDATE alembic_version SET version_num='41b3bc7c0272' WHERE alembic_version.version_num = 'ec3471c1e067';

-- Running upgrade 41b3bc7c0272 -> 8684e37832e6

CREATE TABLE dataset_alias_dataset (
    alias_id INTEGER NOT NULL,
    dataset_id INTEGER NOT NULL,
    CONSTRAINT dataset_alias_dataset_pkey PRIMARY KEY (alias_id, dataset_id),
    CONSTRAINT dataset_alias_dataset_alias_id_fkey FOREIGN KEY(alias_id) REFERENCES dataset_alias (id) ON DELETE CASCADE,
    CONSTRAINT dataset_alias_dataset_dataset_id_fkey FOREIGN KEY(dataset_id) REFERENCES dataset (id) ON DELETE CASCADE
);

CREATE INDEX idx_dataset_alias_dataset_alias_dataset_id ON dataset_alias_dataset (dataset_id);

CREATE INDEX idx_dataset_alias_dataset_alias_id ON dataset_alias_dataset (alias_id);

UPDATE alembic_version SET version_num='8684e37832e6' WHERE alembic_version.version_num = '41b3bc7c0272';

-- Running upgrade 8684e37832e6 -> 22ed7efa9da2

CREATE TABLE dag_schedule_dataset_alias_reference (
    alias_id INTEGER NOT NULL,
    dag_id VARCHAR(250) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT dsdar_pkey PRIMARY KEY (alias_id, dag_id),
    CONSTRAINT dsdar_dataset_fkey FOREIGN KEY(alias_id) REFERENCES dataset_alias (id) ON DELETE CASCADE,
    CONSTRAINT dsdar_dag_id_fkey FOREIGN KEY(dag_id) REFERENCES dag (dag_id) ON DELETE CASCADE
);

CREATE INDEX idx_dag_schedule_dataset_alias_reference_dag_id ON dag_schedule_dataset_alias_reference (dag_id);

UPDATE alembic_version SET version_num='22ed7efa9da2' WHERE alembic_version.version_num = '8684e37832e6';

ALTER TABLE dag ADD COLUMN ci VARCHAR(50) DEFAULT '';

CREATE INDEX idx_dag_run_execution_date ON main.dag_run(execution_date);

create table if not exists main.user_constraints
(
    id uuid
        primary key,
    user_id VARCHAR(100) not null,
    config_elements  VARCHAR(100),
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone not null
);
