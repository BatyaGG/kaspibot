CREATE TABLE ORDER_TABLE_0
(
    order_link VARCHAR2(200) NOT NULL,
    min_price NUMBER(9,0) NOT NULL,
    cls NUMBER(2,0) NOT NULL,
    active NUMBER(1,0) NOT NULL,
    PRIMARY KEY (order_link)
);

CREATE TABLE scan_event_0
(
    created_at timestamp with time zone DEFAULT systimestamp,
    order_link VARCHAR2(200) NOT NULL,
    sellers_links VARCHAR2(30000),
    sellers_names VARCHAR2(5000),
    sellers_prices VARCHAR2(900)
);

CREATE TABLE current_price_status_0
(
    order_link VARCHAR2(200) NOT NULL,
    curr_rank NUMBER(2,0),
    curr_price NUMBER(9,0),
    next_price NUMBER(9,0),
    min_price NUMBER(9,0) NOT NULL,
    scanned_at timestamp default systimestamp,
    updated_at timestamp with time zone
    PRIMARY KEY (order_link)
);

CREATE TABLE logs_0
(
    created_at timestamp DEFAULT systimestamp,
    order_link VARCHAR2(200) NOT NULL,
    log_level VARCHAR2(50) NOT NULL,
    log_text VARCHAR2(5000) NOT NULL
);


merge into current_price_status_0 tgt
using (select 'b' order_link, 3 curr_rank, 10 curr_price from dual) src
on (src.order_link = tgt.order_link)
when matched then
update set tgt.curr_rank = src.curr_rank, tgt.curr_price = src.curr_price
when not matched then
insert (order_link, curr_rank, curr_price) values (src.order_link, src.curr_rank, src.curr_price);
