import sys
import os
import time
import argparse
import psycopg2 as pg
import config
sys.path.append('Customer_data')
from Customer_data.Customers import customers

parser = argparse.ArgumentParser()
parser.add_argument('customer_id', type=str)
args = parser.parse_args()

db = pg.connect(user=config.db_user,
                password=config.db_pass,
                database=config.db,
                host=config.host,
                port=config.port)

cursor = db.cursor()

cursor.execute(f"""CREATE TABLE IF NOT EXISTS _{args.customer_id}_order_table
                (
                    order_link text NOT NULL,
                    min_price integer NOT NULL,
                    cls integer NOT NULL,
                    skip boolean NOT NULL,
                    skip_reason text,
                    iter_no integer NOT NULL,
                    PRIMARY KEY (order_link)
                )""")

cursor.execute(f"""CREATE TABLE IF NOT EXISTS _{args.customer_id}_scan_event
                (
                    order_link text NOT NULL,
                    sellers_links text[] NOT NULL,
                    sellers_prices integer[] NOT NULL,
                    created_at timestamp without time zone DEFAULT now()
                )""")

cursor.execute(f"""CREATE TABLE IF NOT EXISTS _{args.customer_id}_current_price_status
                (
                    order_link text NOT NULL,
                    curr_price integer NOT NULL,
                    next_price integer,
                    PRIMARY KEY (order_link)
                )""")

cursor.execute(f"""CREATE TABLE IF NOT EXISTS _{args.customer_id}_logs
                (
                    created_at timestamp without time zone DEFAULT now(),
                    thread integer NOT NULL,
                    log text NOT NULL
                )""")

cursor.execute(f"truncate _{args.customer_id}_current_price_status")

db.commit()
cursor.close()
db.close()

os.system(f"python3 order_list_to_db.py {args.customer_id} Customer_data/{customers[int(args.customer_id)]['filename']} link price")
time.sleep(5)
