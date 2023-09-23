import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

mysql_config = {
    'host': os.getenv("HOST"),
    'user': os.getenv("USERNAME"),
    'password': os.getenv("PASSWORD"),
    'database': os.getenv("DATABASE")
}

def connect_to_mysql():
    return mysql.connector.connect(**mysql_config)