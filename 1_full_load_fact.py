# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/saktheeshsaktheesh15@gmail.com/utilities

# COMMAND ----------

print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

# DBTITLE 1,Cell 4
dbutils.widgets.text("catalog","FMCG","Catalog")
dbutils.widgets.text("data_source","orders","Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sports-dp-databricks/{data_source}'
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"
print("Base Path:", base_path)
print("landing path:", landing_path)
print("Processed path:",processed_path)

#Define the tables
bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# COMMAND ----------



# COMMAND ----------

df = spark.read.options(header=True, inferSchema=True).csv(f"{landing_path}/*.csv").withColumn("read_timestamp",F.current_timestamp()).select("*","_metadata.file_name","_metadata.file_size")

print("Total Rows:",df.count())
df.show(5)

# COMMAND ----------

display(df.limit(20))

# COMMAND ----------

df.write\
    .format("delta")\
    .option("data.enableChangeDataFeed","true")\
    .mode("append")\
    .saveAsTable(bronze_table)

# COMMAND ----------

files = dbutils.fs.ls(landing_path)
files

# COMMAND ----------

files = dbutils.fs.ls(landing_path)

for file_info in files:
    dbutils.fs.mv(
        file_info.path,
        f"{processed_path}/{file_info.name}",
        True
    )

# COMMAND ----------

df_orders = spark.sql(f"SELECT * FROM {bronze_table}")
df_orders.show(2)

# COMMAND ----------

# Keep only the rows where the order_quantity is present

df_orders=df_orders.filter(F.col("order_qty").isNotNull())

# clean customer_id keep numerics else set to 999999
df_orders=df_orders.withColumn(
    "customer_id",
    F.when(F.col("customer_id").rlike("^[0-9]+$"),F.col("customer_id"))
    .otherwise("999999")
    .cast("string")
)

# Remove the week day name from the day text
# "Tuesday, july 01, 2025" -> "jul 01,2025"
df_orders=df_orders.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"),r"^[A-Za-z]+,\s*","") 
)

# parse order_placement_date using multiple possible formats
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date("order_placement_date","yyyy/MM/dd"),
        F.try_to_date("order_placement_date","dd-MM-yyyy"),
        F.try_to_date("order_placement_date","dd/MM/yyyy"),
        F.try_to_date("order_placement_date","MMMM dd, yyyy"),
    )
)

# Drop duplicates
df_orders=df_orders.dropDuplicates(["order_id","order_placement_date","customer_id","product_id","order_qty"])

# Convert product id to string
df_orders = df_orders.withColumn('product_id',F.col('product_id').cast('string'))

# COMMAND ----------

# check what's the maximum and minimum date
df_orders.agg(
    F.min("order_placement_date").alias("min_date"),
    F.max("order_placement_date").alias("max_date")
).show()


# COMMAND ----------

display(df_orders.limit(20))

# COMMAND ----------

df_products = spark.table("FMCG.silver.products")
display(df_products.limit(50))

# COMMAND ----------

df_joined = df_orders.join(df_products, on="product_id", how="inner").select(df_orders["*"],df_products["product_code"])

display(df_joined.limit(10))

# COMMAND ----------

gold_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold_table

# COMMAND ----------

if not (spark.catalog.tableExists(silver_table)):
    df_joined.write.format("delta").option(
        "delta.enableChangeDataFeed","true"
    ).option("mergeSchema","true").mode("overwrite").saveAsTable(silver_table)

else:
    silver_delta = DeltaTable.forName(spark,silver_table)
    silver_delta.alias("silver").merge(df_joined.alias("bronze"),"silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gold

# COMMAND ----------

df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code,product_id,order_qty as solid_quantity FROM {silver_table};")
df_gold.show(2)

# COMMAND ----------

if not (spark.catalog.tableExists(gold_table)):
    print("creating New Table")
    df_gold.write.format("delta").option(
        "delta.enableChangeDataFeed","true"
    ).option("mergeSchema","true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark,gold_table)
    gold_delta.alias("source").merge(df_gold.alias("gold"),"source.date = gold.date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge with Parent Company

# COMMAND ----------

 df_child = spark.sql(f"SELECT date, product_code, customer_code, solid_quantity FROM {gold_table}")
 df_child.show(10)

# COMMAND ----------

df_child.count()

# COMMAND ----------

# First change the date to first day of the month. 
# 2025-07-12 --> 2025-07-01
# 2025-07-13 --> 2025-07-01
df_monthly =(
    df_child
    # 1. Get month start date (e.g., 2025-11-30 -> 2025-11-01)
    .withColumn("month_start",F.trunc("date","MM")) # or F.date_trunc("month","date").cast("date")

    # 2. Group at monthly grain by month_start + product_code + customer_code
    .groupBy("month_start","product_code","customer_code")
    .agg(
        F.sum("solid_quantity").alias("solid_quantity")
    )
    # 3. Rename month_start back to 'date' to match your target schema
    .withColumnRenamed("month_start","date")
)
display(df_monthly.limit(10))

# COMMAND ----------

df_monthly.count()

# COMMAND ----------

# Rename column to match target table schema
df_monthly_renamed = df_monthly.withColumnRenamed("solid_quantity", "sold_quantity")

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(df_monthly_renamed.alias("child_gold"),"parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code ").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()