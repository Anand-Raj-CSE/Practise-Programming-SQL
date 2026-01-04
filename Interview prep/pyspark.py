''' Q1. DataFrame Basics
You have a PySpark DataFrame orders(order_id, customer_id, order_date, amount).
👉 Write PySpark code to: Select only customer_id, amount.Filter orders where amount > 1000.
Show the first 5 results. '''
df = orders.select("customer_id","amount").fileter(column("amount")>1000).show(5)

"""
Q2. Add Discount Column
You have a DataFrame orders(order_id, customer_id, order_date, amount).
👉 Write PySpark code to add a new column discounted_amount which is 90% of amount 
(i.e., apply a 10% discount). Then display the first 5 rows.
"""
df = orders.withColumn("Discounted_amount",col("amount")*0.9).show(5)
"""
Sum of orders per customer
"""
from pyspark.sql.functions import * 
df = orders.groupBY('customer_id').agg(sum(col('amount')).alias("Total_sales"))\
    .orderBy(desc("Total_sales"))
"""
Task 3: Find top N customers by sales
Description:
Using the total sales per customer (like from Task 2), get the top 3 customers by total_amount.
Output should include: customer_id and total_amount.
"""
df = orders.groupBY('customer_id').agg(sum(col('amount')).alias("Total_sales"))\
    .orderBy(desc("Total_sales")).show(3)
# Through window function
df =orders.groupBY('customer_id').agg(sum(col('amount')).alias("Total_sales"))
ordered_df = df.orderBy(desc("Total_sales"))
ordered_df = ordered_df.withColumn('Ranked_column',rank().over(ordered_df))
ordered_df.show(3)
## Over only expects a window specific object , so cotrect code
df =orders.groupBY('customer_id').agg(sum(col('amount')).alias("Total_sales"))
ordered_df = Window.orderBy(desc("Total_sales"))
ordered_df = ordered_df.withColumn('Ranked_column',rank().over(ordered_df))
ordered_df.show(3)
"""
Task 4: Handle missing data
Description:
You have a DataFrame df with columns:
name (string)
age (integer)
salary (float)
Goal:
Replace null values in age with 0.
Replace null values in salary with the mean salary of the column.
"""
df = orders.fillna(col('age'),0).when(isnull(col('salary'))==true,mean(col('amount')))
mean_salary =  orders.select(mean('salary')).collect()[0][0]
orders.fillna({"age":0},{"salary":mean_salary})

'''
Task 6: Join two DataFrames
Description:
You have two DataFrames:
customers → columns:
id (integer)
name (string)
orders → columns:
customer_id (integer)
amount (float)
Goal: Join them to get a new DataFrame with:
customer_id
name
amount
'''
new_df = customers.join(orders , orders.customer_id = customers.id , 'inner')\
    .select(order.customer_id,customers.name,order.amount)

"""
Task 7: Group and aggregate
Description:
You have a DataFrame orders with columns:
customer_id
amount
Goal:
For each customer, compute:
Average amount → avg_amount
Minimum amount → min_amount
Maximum amount → max_amount
Output should have columns:
customer_id | avg_amount | min_amount | max_amount
"""
df = order.groupBy('customer_id').agg(avg('amount')).alias('avg_amount')\
          .min(amount).alias('min_amount').max(amount).alias('max_amount')
# correct code
df_agg = orders.groupBy('customer_id').agg(avg(col('amount')).alias('avg_amount'),\
    min(col('amount')).alias('min_amount'),
    max(col('amount')).alias('max_amount')
)
"""
Task 8: Rank orders per customer by amount
Description:
You have a DataFrame orders with columns:
customer_id
amount
order_date
Goal:
For each customer, rank their orders by amount in descending order.
Add a new column rank with this rank.
"""
df = order.groupBy('customer_id').agg(sum('amount').alias('Total_amount'))
window_sf = window.orderBy(DESC('Total_amount'))
ranked_df = df.withColumn('Ranked_col',dense_rank().over(window_sf).partitionBy('order_date'))
ranked_df.show()
#Corrected code
window_spec = Window.partitionBy('customer_id').orderBy(Desc('amount'))
ranked_df = orders.withColumn('ranked_col',dense_rank().over(window_spec))
ranked_df.show()
"""
Task 10: Remove duplicates
Description:
You have a DataFrame df with columns:
customer_id
amount
Goal:
Remove duplicate rows based on customer_id so that each customer appears only once.
"""
df = orders.dropDuplicates(['customer_id'])

# running sum in pyspark
df=df.withColumn("running_sum",sum(col('amt')).over(Window.PartitionBy(col('cust_id')).orderBy(col('purchase_dt'))\
                 .rows_between(Window.Unboundedpreceding,Window.currentRow)))