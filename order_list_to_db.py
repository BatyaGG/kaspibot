import pandas as pd
import config
import math
import sys
import argparse
import cx_Oracle
import pandas.io.sql as psql

sys.path.append('Customer_data')
from Customer_data.Customers import customers


parser = argparse.ArgumentParser()
parser.add_argument('customer_id', type=int)
parser.add_argument('orders_col_name', type=str)
parser.add_argument('min_price_col_name', type=str)
args = parser.parse_args()


def correct_link(x):
    return '/'.join(x.split('/')[:-1])


# db = pg.connect(user=config.db_user,
#                 password=config.db_pass,
#                 database=config.db,
#                 host=config.host,
#                 port=config.port)
cx_Oracle.init_oracle_client(config_dir=config.wallet_dir,
                             lib_dir=config.db_lib_dir)
db = cx_Oracle.connect('ADMIN', 'ASD123asdASD123asd', 'dwh_high')

pd.set_option('max_columns', 10)

skip_orders = []

df = pd.read_csv('Customer_data/' + customers[args.customer_id]['filename'], delimiter=';')
# df = pd.read_csv('new_cc.csv', delimiter=';')
# price_col_name = args.min_price_col_name
# price_col_name = 'Минимум цена2'
df = df[[args.orders_col_name, args.min_price_col_name, 'cls']]
# df = df[['Ссылка на товар', price_col_name]]

df[args.orders_col_name] = df[args.orders_col_name].apply(correct_link)
# df['Ссылка на товар'] = df['Ссылка на товар'].apply(correct_link)
print('len of df before', len(df))
df.drop_duplicates(subset=args.orders_col_name, inplace=True)
df = df[df.link!='']
# df.drop_duplicates(subset='Ссылка на товар', inplace=True)
print('len of df after', len(df))
cursor = db.cursor()
cursor.execute(f"TRUNCATE TABLE order_table_{args.customer_id}")

rows = [tuple(list(x) + [1, 0]) for x in df.values]
cursor.executemany(f"INSERT INTO ORDER_TABLE_{args.customer_id} (ORDER_LINK, MIN_PRICE, CLS, ACTIVE, ITER_NO)"
                   f"VALUES (:1,:2,:3,:4,:5)", rows)
db.commit()
cursor.close()
# for i in range(len(df)):
#     row = df.iloc[i]
#     cursor.execute(f"INSERT INTO _{args.customer_id}_order_table (order_link, min_price, skip, iter_no, cls) "
#                    "VALUES(%s, %s, %s, %s, %s)", (row[args.orders_col_name], math.ceil(float(row[args.min_price_col_name])),
#                                                   True if row[args.orders_col_name] in skip_orders else False, 0, int(row['cls'])))
#     db.commit()
#
# cursor.close()
