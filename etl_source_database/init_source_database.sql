drop database if exists datasource;
create database datasource;

\c datasource;

create schema etl;

drop table if exists etl.regions cascade;
create table etl.regions (
    region_id serial primary key,
    region_name varchar(50) unique not null,
    region_local_open time not null,
    region_local_close time not null,
    region_update_timestamp timestamp default current_timestamp
);

drop table if exists etl.industries cascade;
create table etl.industries (
    industry_id serial primary key,
    industry_name varchar(200) not null,
    industry_sector varchar(200) not null,
    database_update_timestamp timestamp default current_timestamp,
    constraint unique_industry unique (industry_name, industry_sector) 
);

drop table if exists etl.sic_industries cascade;
create table etl.sic_industries (
    sic_id int primary key,
    sic_industry varchar(200) not null,
    sic_sector varchar(200) not null,
    database_update_timestamp timestamp default current_timestamp,
    constraint unique_sic_industry unique (sic_industry, sic_sector)
);

drop table if exists etl.exchanges cascade;
create table etl.exchanges (
    exchange_id serial primary key,
    exchange_region_id int not null,
    exchange_name varchar(50) unique not null,
    database_update_timestamp timestamp default current_timestamp,
    constraint fk_exchange_region_id 
    foreign key (exchange_region_id)
    references etl.regions(region_id)
);

drop table if exists etl.companies cascade;
create table etl.companies (
    company_id serial primary key,
    company_exchange_id int not null,
    company_industry_id int, 
    company_sic_id int,
    company_name varchar(200) not null,
    company_ticker varchar(50) not null,
    company_is_delisted boolean not null,
    company_category varchar(200),
    company_currency varchar(50),
    company_location varchar(200),
    database_update_timestamp timestamp default current_timestamp,
    constraint fk_company_exchange foreign key (company_exchange_id) references etl.exchanges(exchange_id),
    constraint fk_company_industry_id foreign key (company_industry_id) references etl.industries(industry_id),
    constraint fk_company_sic_id foreign key (company_sic_id) references etl.sic_industries(sic_id),
    constraint unique_company_delisted unique (company_ticker, company_is_delisted)
);

create index if not exists idx_company_timestamp on etl.companies(database_update_timestamp);
create index if not exists idx_company_exchange_id on etl.companies(company_exchange_id);
create index if not exists idx_exchange_region_id on etl.exchanges(exchange_region_id);
create index if not exists idx_company_industry_id on etl.companies(company_industry_id);
create index if not exists idx_company_sic_id on etl.companies(company_sic_id);