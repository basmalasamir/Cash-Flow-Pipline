# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists cash_flow_project.cash_flow_gold

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC # Dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_date — one row per calendar day across the full project range

# COMMAND ----------

dim_date = spark.sql("""
    SELECT
        CAST(date_format(date, 'yyyyMMdd') AS INT) AS date_key,
        date,
        year(date)        AS year,
        month(date)       AS month,
        dayofmonth(date)  AS day,
        quarter(date)     AS quarter,
        date_format(date, 'MMMM')  AS month_name,
        date_format(date, 'EEEE')  AS day_name,
        date_format(date, 'yyyy-MM') AS month_key,
        CASE WHEN dayofweek(date) IN (1,7) THEN true ELSE false END AS is_weekend
    FROM (
        SELECT explode(sequence(
            to_date('2022-01-01'),
            to_date('2023-12-31'),
            interval 1 day
        )) AS date
    )
""")
(dim_date.write.format("delta").mode("overwrite")
 .saveAsTable("cash_flow_project.cash_flow_gold.dim_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_account — the 3 financial accounts (2 bank + 1 credit card)

# COMMAND ----------

accounts_data = [
    (1, "Checking Main",      "Checking"),
    (2, "Checking Secondary", "Checking"),
    (3, "Credit Card",        "Credit Card"),
]
dim_accounts = spark.createDataFrame(
    accounts_data, ["account_key", "account_name", "account_type"]
)
(dim_accounts.write.format("delta").mode("overwrite")
 .saveAsTable("cash_flow_project.cash_flow_gold.dim_accounts"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_category — income/expense/transfer categories used for classification

# COMMAND ----------

categories_data = [
    (101, "Sales Revenue",    "Income"),
    (102, "COGS",             "Expense"),
    (103, "Operating Expense","Expense"),
    (104, "Payroll",          "Expense"),
    (105, "Marketing",        "Expense"),
    (106, "Utilities",        "Expense"),
    (107, "Supplies & COGS",  "Expense"),
    (108, "Internal Transfer","Transfer"),
    (109, "Other",            "Other"),
]
dim_categories = spark.createDataFrame(
    categories_data, ["category_key", "category_name", "category_type"]
)
(dim_categories.write.format("delta").mode("overwrite")
 .saveAsTable("cash_flow_project.cash_flow_gold.dim_categories"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_employee — one row per unique employee/contractor

# COMMAND ----------

payroll_silver = spark.table("cash_flow_project.cash_flow_silver.gusto_payroll")
dim_employee = (
    payroll_silver
    .select("employee_id", "employee_name", "role", "type", "account")
    .distinct()
    .withColumnRenamed("type", "pay_type")
    .withColumn("employee_key", F.dense_rank().over(Window.orderBy("employee_id")))
)
(dim_employee.write.format("delta").mode("overwrite")
 .saveAsTable("cash_flow_project.cash_flow_gold.dim_employee"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_product — one row per unique product

# COMMAND ----------

sales_silver = spark.table("cash_flow_project.cash_flow_silver.coffee_shop_sales")
dim_product = (
    sales_silver
    .select("product_id", "product_category", "product_type", "product_detail", "unit_price")
    .groupBy("product_id", "product_category", "product_type", "product_detail")
    .agg(F.avg("unit_price").cast("decimal(8,2)").alias("standard_unit_price"))
    .withColumn("product_key", F.dense_rank().over(Window.orderBy("product_id")))
    .orderBy("product_id")
)
(dim_product.write.format("delta").mode("overwrite")
 .saveAsTable("cash_flow_project.cash_flow_gold.dim_product"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Fact tables

# COMMAND ----------

main_silver = spark.table("cash_flow_project.cash_flow_silver.checking_accounts_main")
sec_silver  = spark.table("cash_flow_project.cash_flow_silver.checking_account_secondary")
cc_silver   = spark.table("cash_flow_project.cash_flow_silver.credit_card_account")
dim_date_df = spark.table("cash_flow_project.cash_flow_gold.dim_date")
dim_acc     = spark.table("cash_flow_project.cash_flow_gold.dim_accounts")
dim_cat     = spark.table("cash_flow_project.cash_flow_gold.dim_categories")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. fact_transactions — every ledger line from all 3 money accounts
# MAGIC

# COMMAND ----------

# Tag each ledger with its account name so they can be unioned into one table
main_txns = (
    main_silver
    .withColumn("account_name", F.lit("Checking Main"))
    .select("transaction_id", "date", "description", "category",
            "type", "amount", "signed_amount", "balance", "account_name")
)

sec_txns = (
    sec_silver
    .withColumn("account_name", F.lit("Checking Secondary"))
    .select("transaction_id", "date", "description", "category",
            "type", "amount", "signed_amount", "balance", "account_name")
)

# Credit card rows don't ship with a "category" column - derive one from the vendor name
cc_txns = (
    cc_silver
    .withColumn("category",
        F.when(F.col("vendor") == "Facebook Ads",     "Marketing")
         .when(F.col("vendor") == "Local Print Shop",  "Marketing")
         .when(F.col("vendor") == "Utility Company",   "Utilities")
         .when(F.col("vendor") == "Coffee Supplier",   "Supplies & COGS")
         .when(F.col("vendor") == "Payment to CC",     "Internal Transfer")
         .otherwise("Other"))
    .withColumn("account_name", F.lit("Credit Card"))
    .withColumnRenamed("vendor", "description")
    .select("transaction_id", "date", "description", "category",
            "type", "amount", "signed_amount", "balance", "account_name")
)

# COMMAND ----------

# Union all 3 ledgers into one transaction-grain fact and attach dimension keys
all_txns = main_txns.union(sec_txns).union(cc_txns)
fact_transactions = (
    all_txns
    .join(dim_date_df.select("date", "date_key"), on="date", how="left")
    .join(dim_acc.select("account_name", "account_key"), on="account_name", how="left")
    .join(dim_cat.select("category_name", "category_key"),
          all_txns.category == dim_cat.category_name, how="left")
    .select(
        "transaction_id", "date_key", "date", "account_key", "account_name",
        F.coalesce(F.col("category_key"), F.lit(109)).alias("category_key"),
        F.col("category").alias("category_name"),
        "type", "description",
        F.round("amount", 2).alias("amount"),
        F.round("signed_amount", 2).alias("signed_amount"),
        F.round("balance", 2).alias("balance"),
    )
)
(fact_transactions.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("cash_flow_project.cash_flow_gold.fact_transactions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. fact_monthly_summary — cash + payroll rolled up to one row per month

# COMMAND ----------

# Monthly totals from the main checking account: income + the 2 direct expense categories
monthly_checking = (
    main_silver
    .withColumn("month_key", F.date_format("date", "yyyy-MM"))
    .groupBy("month_key")
    .agg(
        F.sum(F.when(F.col("category") == "Sales Revenue", F.col("amount"))).alias("cash_revenue"),
        F.sum(F.when(F.col("category") == "COGS", F.col("amount"))).alias("cogs_expense"),
        F.sum(F.when(F.col("category") == "Operating Expense", F.col("amount"))).alias("operating_expense"),
        F.last("balance").alias("month_end_balance"),
    )
)

# COMMAND ----------

# Monthly credit card spend by vendor category (Payment to CC transfers excluded)
monthly_cc = (
    cc_silver
    .filter(F.col("type") == "Debit")
    .withColumn("month_key", F.date_format("date", "yyyy-MM"))
    .withColumn("cc_category",
        F.when(F.col("vendor").isin("Facebook Ads", "Local Print Shop"), "Marketing")
         .when(F.col("vendor") == "Utility Company", "Utilities")
         .when(F.col("vendor") == "Coffee Supplier", "Supplies & COGS")
         .otherwise("Other"))
    .groupBy("month_key")
    .agg(
        F.sum("amount").alias("total_cc_spend"),
        F.sum(F.when(F.col("cc_category") == "Marketing", F.col("amount"))).alias("marketing_spend"),
        F.sum(F.when(F.col("cc_category") == "Utilities", F.col("amount"))).alias("utilities_spend"),
        F.sum(F.when(F.col("cc_category") == "Supplies & COGS", F.col("amount"))).alias("supplies_spend"),
    )
)

# COMMAND ----------

# Monthly payroll totals, split by employee vs contractor
monthly_payroll = (
    payroll_silver
    .withColumn("month_key", F.date_format("pay_date", "yyyy-MM"))
    .groupBy("month_key")
    .agg(
        F.sum("amount").alias("total_payroll"),
        F.sum(F.when(F.col("type") == "Employee Pay", F.col("amount"))).alias("employee_payroll"),
        F.sum(F.when(F.col("type") == "Contractor Pay", F.col("amount"))).alias("contractor_payroll"),
    )
)

# COMMAND ----------

# Combine all 4 monthly sources into one row-per-month table and derive the core KPIs
fact_monthly = (
    monthly_checking
    .join(monthly_cc,      on="month_key", how="left")
    .join(monthly_payroll, on="month_key", how="left")
    .fillna(0)
    .withColumn("total_expenses",
        F.round(
            F.coalesce(F.col("cogs_expense"), F.lit(0)) +
            F.coalesce(F.col("operating_expense"), F.lit(0)) +
            F.coalesce(F.col("total_payroll"), F.lit(0)) +
            F.coalesce(F.col("total_cc_spend"), F.lit(0))
        , 2))
    .withColumn("net_cash_flow",
        F.round(F.col("cash_revenue") - F.col("total_expenses"), 2))
    .withColumn("payroll_pct_of_revenue",
        F.when(F.col("cash_revenue") > 0,
               F.round(F.col("total_payroll") / F.col("cash_revenue") * 100, 1))
         .otherwise(F.lit(None)))
    # NEW: quarter, for the "which quarter makes the most money" question
    .withColumn("quarter", F.quarter(F.to_date(F.concat(F.col("month_key"), F.lit("-01")))))
    .withColumn("calendar_year", F.year(F.to_date(F.concat(F.col("month_key"), F.lit("-01")))))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shortage detection 

# COMMAND ----------

monthly_window = Window.orderBy("month_key").rowsBetween(-3, -1)
month_order    = Window.orderBy("month_key")

fact_monthly_with_rolling = (
    fact_monthly
    .withColumn("avg_3m_revenue",  F.avg("cash_revenue").over(monthly_window).cast("decimal(12,2)"))
    .withColumn("avg_3m_expenses", F.avg("total_expenses").over(monthly_window).cast("decimal(12,2)"))
    .withColumn("avg_3m_balance",  F.avg("month_end_balance").over(monthly_window).cast("decimal(12,2)"))
    .withColumn("month_number", F.row_number().over(month_order))
)

# COMMAND ----------

fact_monthly_final = (
    fact_monthly_with_rolling
    .withColumn("shortage_flag",
        F.when(F.col("net_cash_flow") < 0, F.lit("CRITICAL"))
         .when(F.col("month_number") <= 3, F.lit("STARTUP_PERIOD"))
         .when(F.col("month_end_balance") < F.col("avg_3m_expenses") * 1.5, F.lit("WARNING"))
         .when(F.col("total_expenses") > F.col("avg_3m_expenses") * 1.2, F.lit("EXPENSE_SPIKE"))
         .when(F.col("cash_revenue") < F.col("avg_3m_revenue") * 0.8, F.lit("INCOME_DROP"))
         .otherwise(F.lit("HEALTHY"))
    )
    .withColumn("shortage_severity",
        F.when(F.col("shortage_flag") == "CRITICAL", 4)
         .when(F.col("shortage_flag") == "EXPENSE_SPIKE", 3)
         .when(F.col("shortage_flag") == "INCOME_DROP", 2)
         .when(F.col("shortage_flag") == "WARNING", 1)
         .otherwise(0))
    .withColumn("needs_ai_suggestion",
        F.col("shortage_flag").isin("CRITICAL", "EXPENSE_SPIKE", "INCOME_DROP", "WARNING"))
    .orderBy("month_key")
)

(fact_monthly_final.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("cash_flow_project.cash_flow_gold.fact_monthly_summary"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cash_flow_project.cash_flow_gold.fact_monthly_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. fact_sales — one row per POS transaction

# COMMAND ----------

# Join sales to date and (now correctly) to the fixed dim_product surrogate key
dim_product = spark.table("cash_flow_project.cash_flow_gold.dim_product")

fact_sales = (
    sales_silver
    .join(dim_date_df.select("date", "date_key"),
          sales_silver.transaction_date == dim_date_df.date, how="left")
    .join(dim_product.select("product_id", "product_key"), on="product_id", how="left")
    .select(
        "transaction_id", "date_key",
        F.col("transaction_date").alias("date"),
        F.date_format("transaction_date", "yyyy-MM").alias("month_key"),
        "product_key", "product_id", "product_category", "product_type", "product_detail",
        "transaction_qty", "unit_price", "revenue", "hour",
    )
)
(fact_sales.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("cash_flow_project.cash_flow_gold.fact_sales"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cash_flow_project.cash_flow_gold.fact_sales limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. fact_payroll — one row per employee, per month

# COMMAND ----------

fact_payroll = (
    payroll_silver
    .withColumn("month_key", F.date_format("pay_date", "yyyy-MM"))
    .join(dim_employee.select("employee_id", "employee_key"), on="employee_id", how="left")
    .groupBy("month_key", "employee_id", "employee_key", "employee_name", "role", "type")
    .agg(
        F.sum("amount").alias("total_paid"),
        F.count("*").alias("num_payments"),
    ).orderBy("month_key")
)
(fact_payroll.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("cash_flow_project.cash_flow_gold.fact_payroll"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cash_flow_project.cash_flow_gold.fact_payroll limit 5
