import pandas as pd #type:ignore
import mysql.connector #type:ignore
from mysql.connector import Error #type:ignore

def customer_transform(dataframe):
    dataframe=dataframe.drop_duplicates()
    return dataframe

def orders_transform(dataframe):
    dataframe=dataframe.drop_duplicates()
    # dataframe['order_date']=pd.to_datetime(dataframe['order_date'],format='mixed',errors='coerce').dt.date
    dataframe['order_amount']=pd.to_numeric(dataframe['order_amount'],errors='coerce')
    return dataframe

def Merge_tables(dataframe1,dataframe2,column):
    dataframe=dataframe1.merge(dataframe2,on=f"{column}",how='left').fillna({'order_amount': 0,'order_id':pd.NA})
    return dataframe

def aggregations(dataframe,*columns):
    dataframe1=dataframe.groupby(list(columns)).agg(
        Total_orders=('order_id','count'),
        Total_amount=('order_amount','sum'),
        Avg_order_amount=('order_amount','mean')       
    )
    return dataframe1


