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
                logging.warning("ket noi that bai")
                return False
        except Error as e:
            logging.error(f"loi khi ket noi: {e}")
            self.connection = None
            return False

    def execute_sql(self, sql_command: str):
        
        if not self.is_connected():
            logging.error("chua ket noi")
            return None
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql_command)
            return cursor
        except Error as e:
            logging.error(f"loi khi thuc thi querry: {e}")
            return cursor 
        except Exception as e:
            logging.error(f"unknow error: {e}")
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
        else:
             logging.debug("???")


    def is_connected(self) -> bool:
        return self.connection is not None and self.connection.is_connected()