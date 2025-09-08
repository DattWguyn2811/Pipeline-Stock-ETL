create sequence time_key_seq;
create sequence company_key_seq;
create sequence topic_key_seq;
create sequence news_key_seq;
create sequence candle_key_seq;
create sequence news_company_key_seq;
create sequence news_topic_key_seq;

-- Table `dim_time` 
create table if not exists dim_time (
    time_key integer default nextval('time_key_seq') primary key,
    time_date date not null,
    time_day_of_week varchar(10),
    time_month varchar(10),
    time_quarter varchar(10),
    time_year integer
);

-- Table `dim_companies`
create table if not exists dim_companies (
    company_key integer default nextval('company_key_seq') primary key,
    company_name varchar(255),
    company_ticker varchar(10) not null,
    company_is_delisted boolean not null,
    company_category varchar(255),
    company_currency varchar(10) not null,
    company_location varchar(255),
    company_exchange_name varchar(225),
    company_region_name varchar(50) not null,
    company_industry_name varchar(255),
    company_industry_sector varchar(255),
    company_sic_industry varchar(255),
    company_sic_sector varchar(255),
    company_update_time_stamp timestamp default current_timestamp
);

-- Table `dim_topics`
create table if not exists dim_topics (
    topic_key integer default nextval('topic_key_seq') primary key,
    topic_name varchar(255) not null,
    constraint unique_topic unique (topic_name)
);

-- Table `dim_news`
create table if not exists dim_news (
    news_key integer default nextval('news_key_seq') primary key,
    news_title text not null,
    news_url text not null,
    news_time_published varchar(50),
    news_authors varchar[],
    news_summary text,
    news_source text,
    news_overall_sentiment_score double not null,
    news_overall_sentiment_label varchar(255) not null,
    news_time_key integer,
    foreign key (news_time_key) references dim_time(time_key)
);

-- Table `fact_candles`
create table if not exists fact_candles (
    candle_key integer default nextval('candle_key_seq') primary key,
    candle_company_key integer not null,
    candle_volume integer not null,
    candle_volume_weighted double not null,
    candle_open double not null,
    candle_close double not null,
    candle_high double not null,
    candle_low double not null,
    candle_time_stamp varchar(50),
    candle_num_of_trades integer not null,
    candle_is_otc boolean default false,
    candle_time_key integer,
    foreign key (candle_company_key) references dim_companies(company_key),
    foreign key (candle_time_key) references dim_time(time_key)
);

-- Table `fact_news_companies`
create table if not exists fact_news_companies (
    news_company_key integer default nextval('news_company_key_seq'),
    news_company_company_key integer not null,
    news_company_news_key integer not null,
    news_company_relevance_score double not null,
    news_company_ticker_sentiment_score double not null,
    news_company_ticker_sentiment_label varchar(255) not null,
    foreign key (news_company_company_key) references dim_companies(company_key),
    foreign key (news_company_news_key) references dim_news(news_key)
);

-- Table `fact_news_topics`
create table if not exists fact_news_topics (
    news_topic_key integer default nextval('news_topic_key_seq'),
    news_topic_news_key integer not null,
    news_topic_topic_key integer not null,
    news_topic_relevance_score double not null,
    foreign key (news_topic_news_key) references dim_news(news_key),
    foreign key (news_topic_topic_key) references dim_topics(topic_key)
);
