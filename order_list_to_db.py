import pandas as pd
import psycopg2 as pg
import config
import math
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('customer_id', type=str)
parser.add_argument('filename', type=str)
parser.add_argument('orders_col_name', type=str)
parser.add_argument('min_price_col_name', type=str)
args = parser.parse_args()


def correct_link(x):
    return '/'.join(x.split('/')[:-1])


db = pg.connect(user=config.db_user,
                password=config.db_pass,
                database=config.db,
                host=config.host,
                port=config.port)

pd.set_option('max_columns', 10)

skip_orders = []

df = pd.read_csv(args.filename, delimiter=';')
# df = pd.read_csv('new_cc.csv', delimiter=';')
# price_col_name = args.min_price_col_name
# price_col_name = 'Минимум цена2'
df = df[[args.orders_col_name, args.min_price_col_name]]
# df = df[['Ссылка на товар', price_col_name]]

df[args.orders_col_name] = df[args.orders_col_name].apply(correct_link)
# df['Ссылка на товар'] = df['Ссылка на товар'].apply(correct_link)
print('len of df before', len(df))
df.drop_duplicates(subset=args.orders_col_name, inplace=True)
# df.drop_duplicates(subset='Ссылка на товар', inplace=True)
print('len of df after', len(df))
cursor = db.cursor()
cursor.execute(f"TRUNCATE _{args.customer_id}_order_table")
for i in range(len(df)):
    row = df.iloc[i]
    cursor.execute(f"INSERT INTO _{args.customer_id}_order_table (order_link, min_price, skip, iter_no) "
                   "VALUES(%s, %s, %s, %s)", (row[args.orders_col_name], math.ceil(float(row[args.min_price_col_name])),
                                              True if row[args.orders_col_name] in skip_orders else False, 0))
    db.commit()

cursor.close()
