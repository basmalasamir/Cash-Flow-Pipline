# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer
# MAGIC Cleans types, standardizes dates, removes duplicates, and adds the
# MAGIC `signed_amount` column used for cash-flow math.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists cash_flow_project.cash_flow_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper: parse a date column that may appear in more than one format

# COMMAND ----------

def parse_flexible_date(col):
    return F.coalesce(
        F.to_date(col, "yyyy-MM-dd"),
        F.to_date(col, "M/d/yyyy")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. checking_accounts_main

# COMMAND ----------

# Standardize date, derive calendar parts, sign the amount, drop exact-duplicate transactions
df = spark.table("cash_flow_project.cash_flow_bronze.checking_accounts_main")
df_silver = (df
    .withColumn("date", parse_flexible_date(F.col("date")))
    .withColumn("day",   F.dayofmonth("date"))
    .withColumn("month", F.month("date"))
    .withColumn("year",  F.year("date"))
    .withColumn("signed_amount",
        F.when(F.col("type") == "Debit", F.col("amount") * -1)
         .otherwise(F.col("amount")))
    .dropDuplicates(["transaction_id"])
)

# COMMAND ----------

(df_silver.write.format("delta").mode("overwrite")
 .saveAsTable("cash_flow_project.cash_flow_silver.checking_accounts_main"))
display(df_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. checking_account_secondary (payroll transfer account)

# COMMAND ----------

# Same pattern as main account - this account only holds payroll transfer records
df = spark.table("cash_flow_project.cash_flow_bronze.checking_account_secondary")
df_silver = (df
    .withColumn("date", parse_flexible_date(F.col("date")))
    .withColumn("day",   F.dayofmonth("date"))
    .withColumn("month", F.month("date"))
    .withColumn("year",  F.year("date"))
    .withColumn("signed_amount",
        F.when(F.col("type") == "Debit", F.col("amount") * -1)
         .otherwise(F.col("amount")))
    .dropDuplicates(["transaction_id"])
)

# COMMAND ----------

(df_silver.write.format("delta").mode("overwrite")
 .saveAsTable("cash_flow_project.cash_flow_silver.checking_account_secondary"))
display(df_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. coffee_shop_sales (POS transactions)

# COMMAND ----------

# Parse both date and time, compute per-line revenue, dedupe on transaction_id
df = spark.table("cash_flow_project.cash_flow_bronze.coffee_shop_sales")
df_silver = (df
    .withColumn("transaction_date", parse_flexible_date(F.col("transaction_date")))
    .withColumn("year",  F.year("transaction_date"))
    .withColumn("month", F.month("transaction_date"))
    .withColumn("day",   F.dayofmonth("transaction_date"))
    .withColumn("hour",  F.hour("transaction_time"))
    .withColumn("revenue", F.round(F.col("transaction_qty") * F.col("unit_price"), 2))
    .withColumn("time_full", F.date_format("transaction_time", "HH:mm:ss"))
    .dropDuplicates(["transaction_id"])
)

# COMMAND ----------

(df_silver.write.format("delta").mode("overwrite")
 .saveAsTable("cash_flow_project.cash_flow_silver.coffee_shop_sales"))
display(df_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. credit_card_account

# COMMAND ----------

# Same date/sign pattern as the checking accounts
df = spark.table("cash_flow_project.cash_flow_bronze.credit_card_account")
df_silver = (df
    .withColumn("date", parse_flexible_date(F.col("date")))
    .withColumn("year",  F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("day",   F.dayofmonth("date"))
    .withColumn("signed_amount",
        F.when(F.col("type") == "Debit", F.col("amount") * -1)
         .otherwise(F.col("amount")))
    .dropDuplicates(["transaction_id"])
)

# COMMAND ----------

(df_silver.write.format("delta").mode("overwrite")
 .saveAsTable("cash_flow_project.cash_flow_silver.credit_card_account"))
display(df_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. gusto_payroll

# COMMAND ----------

df = spark.table("cash_flow_project.cash_flow_bronze.gusto_payroll")
df_silver = (df
    .withColumn("pay_date", parse_flexible_date(F.col("pay_date")))
    .withColumn("year",  F.year("pay_date"))
    .withColumn("month", F.month("pay_date"))
    .withColumn("day",   F.dayofmonth("pay_date"))
    .withColumn("silver_dq_flag",
        F.when((F.col("year") == 2022) & (F.col("month") == 1), F.lit(None))  
         .otherwise(F.lit(None)))
    .dropDuplicates(["employee_id", "pay_date"])
)

# COMMAND ----------

(df_silver.write.format("delta").mode("overwrite")
 .saveAsTable("cash_flow_project.cash_flow_silver.gusto_payroll"))
display(df_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation: did any dates fail to parse?
# MAGIC Run this after every re-ingestion. If any count here is > 0, the raw CSV has
# MAGIC a date format we haven't accounted for yet — add it to `parse_flexible_date`.

# COMMAND ----------

checks = {
    "checking_accounts_main": "date",
    "checking_account_secondary": "date",
    "coffee_shop_sales": "transaction_date",
    "credit_card_account": "date",
    "gusto_payroll": "pay_date",
}
for table, date_col in checks.items():
    n_nulls = (spark.table(f"cash_flow_project.cash_flow_silver.{table}")
               .filter(F.col(date_col).isNull()).count())
    print(f"{table}: {n_nulls} unparsed dates")
