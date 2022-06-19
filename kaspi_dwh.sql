CREATE TABLE ORDER_TABLE_0
(
    order_link VARCHAR2(200) NOT NULL,
    min_price NUMBER(9,0) NOT NULL,
    cls NUMBER(2,0) NOT NULL,
    skip NUMBER(1,0) NOT NULL,
    skip_reason VARCHAR2(50),
    iter_no NUMBER(4,0) NOT NULL,
    PRIMARY KEY (order_link)
);

CREATE TABLE scan_event_0
(
    order_link VARCHAR2(200) NOT NULL,
    sellers_links VARCHAR2(30000) NOT NULL,
    sellers_prices VARCHAR2(900) NOT NULL,
    created_at timestamp DEFAULT systimestamp
);

CREATE TABLE current_price_status_0
(
    order_link VARCHAR2(200) NOT NULL,
    curr_price NUMBER(9,0) NOT NULL,
    next_price NUMBER(9,0),
    PRIMARY KEY (order_link)
);

CREATE TABLE logs_0
(
    created_at timestamp DEFAULT systimestamp,
    log_text VARCHAR2(500) NOT NULL
);