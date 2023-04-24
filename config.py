# db_lib_dir = '/Users/batyagg/drivers/instantclient_19_8'
# wallet_dir = '/Users/batyagg/drivers/Wallet_dwh'

# db_lib_dir = '/main/drivers/instantclient_19_8'
# wallet_dir = '/main/drivers/Wallet_dwh'

db_user = 'eldo'
db_pass = '123123123'
db = 'eldo'
host = 'localhost'
port = 5432

merchant_id = 1
# num_tabs = 10
timeout_tab = 2 * 60
timeout_end = 3 * 60 * 60
price_step = 2
headless = True


#  DOCKER_BUILDKIT=0 docker build --platform windows/amd64 -t kaspibot . docker tag kaspibot batyagg/kaspibot:eldo12