import traceback

from status_codes import *


class UserFunctions:
    def __init__(self, db):
        self.db = db

    def create_new_user(self, email, phone, pass_hashed):
        # return status: 0 - success, 1 - already_exists, 2 - bad error
        try:
            cursor = self.db.cursor(row_factory=dict)
            record = cursor.execute(f"select * from merchants where email = '{email}'").fetchone()
            assert phone == record['phone'], 'phone and email combination must be unique'
            if len(record) == 1:
                self.write_logs_out(record['merchant_id'], 'DEBUG', USER_ALREADY_EXISTS,
                                    f"merchant_id: {record['merchant_id']}, email: {email}\n phone: {phone}")
                cursor.close()
                return 1
            else:
                cursor.execute('insert into merchants (email, phone, password)'
                               'values (%s, %s, %s)', (email, phone, pass_hashed))
                self.db.commit()
                record = cursor.execute(f"select * from merchants where email = '{email}'").fetchone()
                self.write_logs_out(record['merchant_id'], 'DEBUG', USER_CREATED,
                                    f"merchant_id: {record['merchant_id']}, email: {email}\n phone: {phone}")
                cursor.close()
                return 0
        except:
            self.write_logs_out(None, 'ERROR', USER_CREATE_ERROR, traceback.format_exc())
            return 2

    def add_user_name(self, merchant_id, name):
        cursor = self.db.cursor()
        cursor.execute(f"update table ")

    def write_logs_out(self, merchant_id, lvl, status_code, text, write_db=True):
        print('_______________________')
        print(merchant_id)
        print(lvl)
        print(status_code)
        print(text)
        print()
        if write_db:
            cursor = self.db.cursor()
            cursor.execute(f"INSERT INTO LOGS (MERCHANT_ID, LOG_LEVEL, STATUS_CODE, LOG_TEXT) "
                           "VALUES (%s, %s, %s, %s) ", (merchant_id, lvl, status_code, text))
            self.db.commit()
            cursor.close()


# if __name__ == '__main__':
