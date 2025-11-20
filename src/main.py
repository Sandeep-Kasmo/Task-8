import pandas as pd #type:ignore

import mysql.connector #type:ignore

from mysql.connector import Error #type:ignore

from extract import *

from transform import *

from load import *

def main():



    #connect to MySQL

    establish_connection()



    #load the customers and orders data and print it

    customer_df=get_customers('select customer_id,name from customer')

    if customer_df is not None:

         print(customer_df)  

    orders_df=get_orders('select order_id,customer_id,order_amount from orders')

    if orders_df is not None:

        print(orders_df)



    #perform transformations, merge both tables and print it

    transform_customers_df=customer_transform(customer_df)

    transform_orders_df=orders_transform(orders_df)

    try:

        if transform_customers_df is not None and transform_orders_df is not None:

            customer_orders_df=Merge_tables(transform_customers_df,transform_orders_df,'customer_id')

            print(f"Customer Orders Data:\n\n{customer_orders_df}\n")

    except Error as e:

        print(f"Error merging tables {e}")

    

    #perform aggregations

    summary=aggregations(customer_orders_df,'customer_id','name')
    summary = summary.reset_index()
    print(summary.sort_values(by='customer_id'))



    #creating tables and inserting the data in the SQL database

    create_aggreagetion(summary,'SummaryTable')

    # insert_data_with_iterrows(summary,'SummaryTable')



    #close the connection

    close_connection()




main()
