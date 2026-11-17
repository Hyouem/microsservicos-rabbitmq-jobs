create table if not exists jobs (
 id uuid primary key,
 payload varchar(1000) not null,
 status varchar(20) not null check(status in ('PENDING','PROCESSING','COMPLETED','FAILED')),
 attempts integer not null default 0,
 error text,
 created_at timestamptz not null default now(),
 updated_at timestamptz not null default now()
);
