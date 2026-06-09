# Databricks notebook source
# MAGIC %md
# MAGIC ### Bronze Processing

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/saktheeshsaktheesh15@gmail.com/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog","FMCG","catalog")
dbutils.widgets.text("data_source","customers","Data source")

# COMMAND ----------

catalog=dbutils.widgets.get("catalog")
data_source=dbutils.widgets.get("data_source")
base_path=f's3://sports-dp-databricks/{data_source}/*.csv'
print(catalog,data_source)
print(base_path)
# spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{data_source}")
# spark.sql(f"USE {catalog}.{data_source}")
# display(spark.sql(f"SHOW TABLES"))
# display

# COMMAND ----------

# DBTITLE 1,Cell 6
df=(
    spark.read.format("csv")
    .option("header",True)
    .option("inferschema",True)
    .load(base_path)
    .withColumn("read_timestamp",F.current_timestamp())
    .select("*","_metadata.file_name","_metadata.file_size")
)
display(df.limit(10))

# COMMAND ----------

# print check data type
df.printSchema()

# COMMAND ----------

df.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed", "true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Processing

# COMMAND ----------

df_bronze= spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
df_bronze.show(10)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

df_duplicates = df_bronze.groupBy("customer_id").count().filter(F.col("count")>1)
display(df_duplicates)

# COMMAND ----------

print('Rows before duplicates dropped:',df_bronze.count())
df_silver = df_bronze.dropDuplicates(["customer_id"])
print('Rows after duplicates dropped:',df_silver.count())

# COMMAND ----------

#Check those values
display(
    df_silver.filter(F.col("customer_name")!=F.trim(F.col("customer_name")))
)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_name",
    F.trim(F.col("customer_name"))
)

# COMMAND ----------

#Check those values
display(
    df_silver.filter(F.col("customer_name")!=F.trim(F.col("customer_name")))
)

# COMMAND ----------

df_silver.select("city").distinct().show()

# COMMAND ----------

# Typos to --> correct  names
city_mapping ={
    'Bengalore':'Bengaluru',
    'Bengaluruu':'Bengaluru',
    'Hyderabadd':'Hyderabad',
    'Hyderbad':'Hyderabad',
    'NewDelhee':'New Delhi',
    'NewDelhi':'New Delhi',
    'NewDheli':'New Delhi'
}

allowed=["Bengaluru","Hyderabad","New Delhi"]

df_silver = (
    df_silver
    .replace(city_mapping,subset=["city"])
    .withColumn(
        "city",
        F.when(F.col("city").isNull(),None)
         .when(F.col("city").isin(allowed),F.col("city"))
         .otherwise(None)
    )
)

# sanity check
df_silver.select('city').distinct().show()

# COMMAND ----------

df_silver.select("customer_name").distinct().show()

# COMMAND ----------

# Title case fix
df_silver = df_silver.withColumn(
    "customer_name",
    F.when(F.col("customer_name").isNull(),None)
     .otherwise(F.initcap("customer_name"))
)
df_silver.select("customer_name").distinct().show()

# COMMAND ----------

df_silver.filter(F.col("city").isNull()).show(truncate=False)

# COMMAND ----------

null_customer_names = ['Sprintx Nutrition','Zenathlete Foods','Primefuel Nutrition','Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

# Business confirmation note: City corrections confirmed by business team
customer_city_fix ={
    # Sprintx Nutrition
    789403 : "New Delhi",

    # Zenathlete Foods
    789420 : "Bengaluru",

    # Primefuel Nutrition
    789521 : "Hyderabad",

    # Recovery Lane
    789603 : "Hyderabad"
}

df_fix = spark.createDataFrame(
    [(k,v) for k, v in customer_city_fix.items() ],
    ["customer_id","fixed_city"]
)
display(df_fix)

# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix,"customer_id","left")
    .withColumn(
        "city",
        F.coalesce("city","fixed_city")  # Replace null with fixed city
    )
    .drop("fixed_city")
)
display(df_silver)

# COMMAND ----------

# Sanity check
null_customer_names = ['Sprintx Nutrition','Zenathlete Foods','Primefuel Nutrition','Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id",F.col("customer_id").cast("string"))
print(df_silver.printSchema())

# COMMAND ----------

df_silver = (
    df_silver
    # Build final customer column: "customerName-City" or "customerName-unknown"
    .withColumn(
        "customer",
        F.concat_ws("-","customer_name",F.coalesce(F.col("city"),F.lit("Unknown")))
    )

    # Static attributes aligned with parent data model
    .withColumn("market",F.lit("India"))
    .withColumn("platform",F.lit("Sports bar"))
    .withColumn("channel",F.lit("Acquisition")) 
)
display(df_silver.limit(5))

# COMMAND ----------

# DBTITLE 1,Cell 29
df_silver.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .option("mergeSchema","true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Processing

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")

# Take req cols only
# "customer_id,customer_name,city, read_timestamp, file_name, file_size, customer, market, platform, channel"
df_gold = df_silver.select("customer_id","customer_name","city","customer","market","platform","channel")


# COMMAND ----------

# DBTITLE 1,Write to Gold table
df_gold.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

delta_table = DeltaTable.forName(spark,"FMCG.gold.dim_customers")
df_child_customers= spark.table("FMCG.gold.sb_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

delta_table.alias("target").merge(
source = df_child_customers.alias("source"),
condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

