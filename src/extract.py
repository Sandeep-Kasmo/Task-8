import pandas as pd #type:ignore
import mysql.connector #type:ignore
from mysql.connector import Error #type:ignore
conn=None
def establish_connection():
    try:
        global conn
        conn=mysql.connector.connect(
            host='localhost',
            user='root',
            password='sandeep123@M',
            database='PythonLearningDB'
        )
        if conn.is_connected():
            print('Connected to MySQL successfully')
            return conn
    except Error as e:
        print(f"Error connecting to SQL{e}")
        return None

def get_customers(query):
    try:
        if conn is None or conn.is_connected==False:
            try:
                establish_connection()
            except Error as e:
                print(f'Error {e}')
                return None
        dataframe=pd.read_sql(query,conn)
        return dataframe
    except Error as e:
        print(f"Error reading table data:{e}")
        return None

def get_orders(query):
    try:
        if conn is None or conn.is_connected==False:
            try:
                establish_connection()
            except Error as e:
                print(f"Error:{e}")
                return None
        dataframe=pd.read_sql(query,conn)
        return dataframe
    except Error as e:
        print(f"Error reading orders data:{e}")
        return None

def close_connection():
    try:
        if conn:
            conn.close()
            print('Closed connection successfully')
    except Error as e:
        print(f"Error closing connection:{e}")
    


