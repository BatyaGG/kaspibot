import pandas as pd
import psycopg2 as pg
import config
import math


def correct_link(x):
    return '/'.join(x.split('/')[:-1])


db = pg.connect(user=config.db_user,
                password=config.db_pass,
                database=config.db,
                host=config.host,
                port=config.port)

pd.set_option('max_columns', 10)

skip_orders = []

df = pd.read_csv('order_minprice.csv', delimiter=';', index_col=0)
df = df[['Ссылка на товар', 'Минимум цена']]

df['Ссылка на товар'] = df['Ссылка на товар'].apply(correct_link)
print('len of df before', len(df))
df.drop_duplicates(subset='Ссылка на товар', inplace=True)
print('len of df after', len(df))
cursor = db.cursor()
cursor.execute("TRUNCATE order_table")
for i in range(len(df)):
    row = df.iloc[i]
    cursor.execute("INSERT INTO order_table (order_link, min_price, skip, iter_no) "
                   "VALUES(%s, %s, %s, %s)", (row['Ссылка на товар'], math.ceil(float(row['Минимум цена'].replace(',', '.'))),
                                              True if row['Ссылка на товар'] in skip_orders else False, 0))
    db.commit()

cursor.close()
