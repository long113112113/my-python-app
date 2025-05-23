import mysql.connector
from mysql.connector import Error
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DatabaseManager:

    def __init__(self, host: str, port: int, initial_db: str | None = None):
        self.host = host
        self.port = port
        self.initial_db = initial_db
        self.connection = None
        self.dynamic_user = None

    def connect(self, username: str, password: str) -> bool:
        logging.info(f"dang ket noi den mysql ({self.host}:{self.port}) bang user: {username}...")
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=username,
                password=password,
                database=self.initial_db,
               
            )
            if self.connection.is_connected():
                self.dynamic_user = username
                logging.info("-> ket noi thanh cong")
                return True
            else:
                logging.warning("ket noi khong thanh cong (sau khi connect tra ve)")
                self.connection = None 
                return False
        except Error as e:
            logging.error(f"loi khi ket noi: {e}")
            self.connection = None
            return False
        except Exception as e: 
            logging.error(f"loi khong mong doi khi ket noi: {e}")
            self.connection = None
            return False


    def execute_sql(self, sql_command: str): 
        if not self.is_connected():
            logging.error("chua ket noi, khong the thuc thi.")
            return None

        cursor = None 
        try:
            cursor = self.connection.cursor()
            logging.debug(f"Executing SQL: {sql_command[:100]}...") 
            cursor.execute(sql_command)
            logging.debug("SQL executed successfully.")
            return cursor
        except Error as e:
            logging.error(f"loi khi thuc thi SQL: '{sql_command[:100]}...': {e}")
            if cursor:
                try:
                    cursor.close()
                except Error as ce:
                    logging.warning(f"Loi khi dong cursor sau khi execute loi: {ce}")
            return None
        except Exception as e:
            logging.error(f"loi khong xac dinh khi thuc thi SQL: {e}")
            if cursor:
                try:
                    cursor.close()
                except Error as ce:
                    logging.warning(f"Loi khi dong cursor sau loi khong xac dinh: {ce}")
            return None 

    def commit(self):
        if self.is_connected():
            try:
                self.connection.commit()
                logging.debug("Transaction committed.")
            except Error as e:
                logging.error(f"loi khi commit transaction: {e}")

    def rollback(self):
        if self.is_connected():
            try:
                self.connection.rollback()
                logging.info("transaction rollback.")
            except Error as e:
                logging.error(f"loi khi rollback: {e}")

    def close(self):
        if self.connection and self.connection.is_connected():
            try:
                dynamic_user_copy = self.dynamic_user
                self.connection.close()
                logging.info(f"dong dong ket noi user: {dynamic_user_copy}.")
                self.connection = None
                self.dynamic_user = None
            except Error as e:
                logging.error(f"khong the dong ket noi {e}")



    def is_connected(self) -> bool:
        return self.connection is not None and self.connection.is_connected()