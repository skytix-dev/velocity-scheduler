CREATE TABLE Schedulers (
    framework_id text primary key,
    registered timestamp with time zone not null,
    recovered_time timestamp with time zone not null,
    recovering boolean default false not null
);

CREATE TABLE Tasks (
    task_id text primary key,
    framework_id text not null,
    task_created timestamp with time zone not null,
    allocated boolean default false not null,
    name text not null,
    command text not null,
    image text not null,
    forcePull boolean default false not null,
    cpus real not null,
    mem real not null,
    gpus real not null,
    disk real not null
);

CREATE TABLE TaskStateHistory (
    task_id text primary key,
    event_time timestamp with time zone not null,
    state text not null
)

