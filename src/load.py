import pandas as pd #type:ignore
import mysql.connector #type:ignore
from mysql.connector import Error #type:ignore
from extract import * # NOTE: extract.py must define 'establish_connection' to return the conn object, 
# and 'conn' must be a global variable (conn=None) defined outside these functions.

def create_aggreagetion(dataframe, table_name):
    global conn
    cursor=None
    
    # --- 1. Connection Check and Assignment ---
    if conn is None or not conn.is_connected():
        try:
            # FIX 1: Assign return value and use correct function call syntax
            new_conn = establish_connection() 
            if new_conn is None:
                print("Error: Could not establish connection.")
                return False
            conn = new_conn # Update global conn
        except Error as e:
            print(f"Error establishing connection: {e}")
            return False
    
    # CRITICAL FIX 2: Check conn is valid before creating cursor
    if conn is None:
        print("Table creation failed: Connection is None.")
        return False
        
    cursor=conn.cursor()
    
    # 2. Clean Column Names (Necessary)
    dataframe.columns = [
        col.strip().replace('(', '').replace(')', '').replace('.', '_').replace('-', '_').replace(' ', '_')
        for col in dataframe.columns
    ]
    columns = dataframe.columns.tolist()

    try:
        # 3. Dynamic Column Definition with Type Guessing (Robust DDL)
        column_defs = []
        for i, col in enumerate(columns):
            sql_type = "VARCHAR(255)"
            
            # IMPROVED: Use appropriate numerical types for aggregated data
            if dataframe[col].dtype in ['float64', 'float32', 'float']:
                sql_type = "FLOAT" 
            elif dataframe[col].dtype in ['int64', 'int32', 'int', 'Int64']:
                sql_type = "BIGINT"
            
            # Assume the first column is the PRIMARY KEY (e.g., 'customer_id')
            constraints = " PRIMARY KEY" if i == 0 else ""
            column_defs.append(f"`{col}` {sql_type}{constraints}")

        column_definitions = ", ".join(column_defs)
        
        # 4. Execute DDL Query (Correct Syntax)
        cursor.execute(
            f''' 
            CREATE TABLE IF NOT EXISTS {table_name}({column_definitions})
            '''
        )
        conn.commit()
        print(f"Table '{table_name}' created or verified successfully with {len(columns)} columns.")
        return True
        
    except Error as e:
        print(f"Error creating table: {e}")
        return False
        
    finally:
        if cursor:
            cursor.close()


def insert_data_with_iterrows(dataframe, table_name):
    global conn
    cursor = None
    
    # FIX 5: Ensure connection is verified and assigned correctly
    if conn is None or not conn.is_connected():
        try:
            new_conn = establish_connection()
            if new_conn is None:
                return False
            conn = new_conn
        except Error as e:
            print(f"Error establishing connection: {e}")
            return False

    if conn is None: # Check again after attempt
        print("Insertion failed: Connection is None.")
        return False
        
    cursor = conn.cursor()
    columns = dataframe.columns.tolist()
    placeholders = ', '.join(['%s'] * len(columns))
    
    # ... rest of the insertion logic (which was mostly correct) ...
    try:
        insert_query = f"""
            INSERT INTO {table_name} ({', '.join([f'`{col}`' for col in columns])})
            VALUES ({placeholders})
        """
        
        for index, row in dataframe.iterrows():          
            data_values = row.values.tolist()
            cursor.execute(insert_query, data_values)
            
        conn.commit()
        print(f"Successfully inserted {len(dataframe)} rows into {table_name} using iterrows.")
        return True
        
    except Error as e:
        print(f"Error inserting data into {table_name}: {e}")
        if conn:
            conn.rollback() 
        return False
        
    finally:
        if cursor:
            cursor.close()