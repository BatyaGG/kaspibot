import argparse
import psycopg2 as pg
import pandas.io.sql as psql

import config

parser = argparse.ArgumentParser()
parser.add_argument('db', type=str)
args = parser.parse_args()

db = pg.connect(user=config.db_user,
                password=config.db_pass,
                database=args.db,
                host=config.host,
                port=config.port)
orders = psql.read_sql('SELECT * from order_table', db)
db.close()
print(len(orders))
