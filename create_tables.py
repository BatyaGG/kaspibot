import psycopg2 as pg

db = pg.connect(user='eldo',
                password='123123123',
                database='eldo',
                host='localhost',
                port=5432)

cursor = db.cursor()
cursor.execute('''

create table if not exists merchants (
merchant_id serial,
name varchar(30),
email varchar(30),
phone varchar(20),
password varchar(30),
kaspi_id integer,
kaspi_name varchar(30),
kaspi_login varchar(30),
kaspi_password varchar(30),
address_tab varchar(100),
orders_link text,
last_active timestamp with time zone,
primary key (merchant_id),
constraint uq_email unique (name)
);

create table if not exists order_table (
merchant_id smallint not null,
order_link varchar(200),
min_price int,
active boolean,
cls smallint,
primary key (merchant_id, order_link),
constraint fk_users_merchant_id foreign key (merchant_id) references merchants (merchant_id)
);

create table if not exists order_fact (
merchant_id smallint not null,
order_link varchar(200),
main_includes boolean,
image_link text,
order_name text,
created_at timestamp with time zone DEFAULT now(),
primary key (merchant_id, order_link),
constraint fk_users_merchant_id foreign key (merchant_id) references merchants (merchant_id)
);

create table if not exists order_archive (
merchant_id smallint not null,
order_link varchar(200),
main_includes boolean,
image_link text,
order_name text,
created_at timestamp with time zone DEFAULT now(),
primary key (merchant_id, order_link),
constraint fk_users_merchant_id foreign key (merchant_id) references merchants (merchant_id)
);

create table if not exists order_status (
merchant_id smallint not null,
order_link varchar(200),
ranking smallint,
curr_price int,
next_price int,
min_price int,
is_alone boolean,
scanned_at timestamp with time zone DEFAULT now(),
updated_at timestamp with time zone,
primary key (merchant_id, order_link),
constraint fk_users_merchant_id foreign key (merchant_id) references merchants (merchant_id)
);

create table if not exists scan_event (
merchant_id smallint not null,
order_link varchar(200),
created_at timestamp with time zone default now(),
sellers_links text,
sellers_names text,
sellers_prices text,
constraint fk_users_merchant_id foreign key (merchant_id) references merchants (merchant_id)
);

create table if not exists logs (
merchant_id smallint,
created_at timestamp with time zone DEFAULT now(),
order_link character varying(200),
log_level character varying(20) NOT NULL,
log_status smallint NOT NULL,
log_text character varying(5000) NOT NULL
);
''')

db.commit()
cursor.close()
cursor = db.cursor()
cursor.execute('''
insert into merchants(name, kaspi_login, kaspi_password) values ('eldo', 'nisay87596@rubeshi.com', 'Galym4@lisGala')
on conflict (name) do nothing;
''')
db.commit()
cursor.close()
