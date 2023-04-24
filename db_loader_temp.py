import sys
import config
import psycopg2 as pg
import pandas as pd

# sys.path.append('Customer_data')
# from Customer_data.Customers import customers

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_colwidth', None)


def correct_link(x):
    x = x.split('?')[0]
    if x[-1] == '/':
        x = x[:-1]
    return x


def create_tables_and_load(customer_id, db):
    filename = 'eldo_csv.csv'
    # filename = 'anubat_data.csv'
    # cx_Oracle.init_oracle_client(config_dir='/Users/batyagg/drivers/Wallet_dwh',
    #                              lib_dir="/Users/batyagg/drivers/instantclient_19_8")
    # db = cx_Oracle.connect('ADMIN', 'ASD123asdASD123asd', 'dwh_high')

    df = pd.read_csv('Customer_data/' + filename, delimiter=';')
    # df = pd.read_csv('new_cc.csv', delimiter=';')
    # price_col_name = args.min_price_col_name
    # price_col_name = 'Минимум цена2'
    df = df[['link', 'price', 'cls', 'active']]
    print(df)
    # df = df[['Ссылка на товар', price_col_name]]

    df['link'] = df['link'].apply(correct_link)
    # df['Ссылка на товар'] = df['Ссылка на товар'].apply(correct_link)
    print('len of df before', len(df))
    print(df.loc[df.duplicated(subset=['link'], keep=False)].sort_values(by=['link'])[['link', 'price']])
    df.drop_duplicates(subset='link', inplace=True)
    df.dropna(inplace=True)
    df = df[df.link != '']
    # df.drop_duplicates(subset='Ссылка на товар', inplace=True)
    print('len of df after', len(df))
    cursor = db.cursor()
    # cursor.execute(f"TRUNCATE TABLE _{customer_id}_order_table")

    # rows = [tuple(list(x) + [1]) for x in df.values]
    # for x in df.values:
    #     print(list(x) + [1])
    # rows = ','.join(cursor.mogrify("(%s, %s, %s, %s)", list(x) + [1]) for x in df.values)
    # rows = ','.join([str(tuple(list(x) + [True])) for x in df.values])
    rows = [str(tuple([customer_id, x[0], x[1], bool(x[3]), x[2]])) for x in df.values]
    # cursor.executemany(f"INSERT INTO _{customer_id}_ORDER_TABLE (ORDER_LINK, MIN_PRICE, CLS, ACTIVE)"
    #                    f"VALUES (:1,:2,:3,:4)", rows)
    # print(rows)
    # print(f"INSERT INTO ORDER_TABLE (MERCHANT_ID, ORDER_LINK, MIN_PRICE, ACTIVE, CLS)"
    #       f"VALUES " + rows)
    cursor.execute(f'delete from order_table where merchant_id = {customer_id}')
    for i, j in zip(list(range(0, len(rows), 100)), list(range(100, len(rows) + 200, 100))):
        print(i, j)
        cursor.execute(f"INSERT INTO ORDER_TABLE (MERCHANT_ID, ORDER_LINK, MIN_PRICE, ACTIVE, CLS) "
                       f"VALUES " + ','.join(rows[i:j]))

    db.commit()
    cursor.close()


if __name__ == '__main__':
    db = pg.connect(user='eldo',
                    password='123123123',
                    database='eldo',
                    host='localhost',
                    port=5432)
    create_tables_and_load(config.merchant_id, db)
