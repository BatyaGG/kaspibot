import cx_Oracle
import pandas.io.sql as psql

cx_Oracle.init_oracle_client(config_dir='/Users/batyagg/drivers/Wallet_dwh',
                             lib_dir="/Users/batyagg/drivers/instantclient_19_8")
db = cx_Oracle.connect('ADMIN', 'ASD123asdASD123asd', 'dwh_high')
df = psql.read_sql('select * from asdqwe', db)
print(df)


# ip = 'adb.ap-singapore-1.oraclecloud.com'
# port = 1522
# SID = 'g3f0d1b7d18755f_dwh_high.adb.oraclecloud.com'
# dsn_tns = cx_Oracle.makedsn(ip, port, SID)
#
# db = cx_Oracle.connect('ADMIN', 'ASD123asdASD123asd', dsn_tns)

# db = cx_Oracle.connect('(description= (retry_count=20)(retry_delay=3)(address=(protocol=tcps)(port=1522)(host=adb.ap-singapore-1.oraclecloud.com))(connect_data=(service_name=g3f0d1b7d18755f_dwh_high.adb.oraclecloud.com))(security=(ssl_server_cert_dn="CN=adb.ap-singapore-1.oraclecloud.com, OU=Oracle ADB SINGAPORE, O=Oracle Corporation, L=Redwood City, ST=California, C=US")))')
# libnnz19.dylib