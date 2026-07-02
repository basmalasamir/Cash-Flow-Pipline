# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Views

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive Overview page

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Month-by-month revenue, expenses, net cash flow, and month-over-month change
# MAGIC CREATE OR REPLACE VIEW cash_flow_project.cash_flow_gold.vw_monthly_financial_overview AS
# MAGIC SELECT
# MAGIC     month_key,
# MAGIC     calendar_year,
# MAGIC     quarter,
# MAGIC     ROUND(cash_revenue, 2)      AS revenue,
# MAGIC     ROUND(total_expenses, 2)    AS total_expenses,
# MAGIC     ROUND(net_cash_flow, 2)     AS net_cash_flow,
# MAGIC     ROUND(month_end_balance, 2) AS closing_balance,
# MAGIC     ROUND(
# MAGIC         (cash_revenue - LAG(cash_revenue) OVER (ORDER BY month_key))
# MAGIC         / NULLIF(LAG(cash_revenue) OVER (ORDER BY month_key), 0) * 100, 1
# MAGIC     ) AS revenue_mom_change_pct,
# MAGIC     ROUND(
# MAGIC         (total_expenses - LAG(total_expenses) OVER (ORDER BY month_key))
# MAGIC         / NULLIF(LAG(total_expenses) OVER (ORDER BY month_key), 0) * 100, 1
# MAGIC     ) AS expenses_mom_change_pct
# MAGIC FROM cash_flow_project.cash_flow_gold.fact_monthly_summary
# MAGIC ORDER BY month_key

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Which quarter makes the most / least money, across both years
# MAGIC CREATE OR REPLACE VIEW cash_flow_project.cash_flow_gold.vw_quarterly_performance AS
# MAGIC SELECT
# MAGIC     calendar_year,
# MAGIC     quarter,
# MAGIC     CONCAT('Q', quarter, ' ', calendar_year) AS quarter_label,
# MAGIC     ROUND(SUM(cash_revenue), 2)    AS total_revenue,
# MAGIC     ROUND(SUM(total_expenses), 2)  AS total_expenses,
# MAGIC     ROUND(SUM(net_cash_flow), 2)   AS total_net_cash_flow,
# MAGIC     ROUND(AVG(cash_revenue), 2)    AS avg_monthly_revenue,
# MAGIC     RANK() OVER (ORDER BY SUM(cash_revenue) DESC) AS revenue_rank
# MAGIC FROM cash_flow_project.cash_flow_gold.fact_monthly_summary
# MAGIC GROUP BY calendar_year, quarter
# MAGIC ORDER BY calendar_year, quarter

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2022 vs 2023 side by side - is the business actually growing?
# MAGIC CREATE OR REPLACE VIEW cash_flow_project.cash_flow_gold.vw_yoy_comparison AS
# MAGIC SELECT
# MAGIC     calendar_year,
# MAGIC     ROUND(SUM(cash_revenue), 2)          AS total_revenue,
# MAGIC     ROUND(SUM(total_expenses), 2)        AS total_expenses,
# MAGIC     ROUND(SUM(net_cash_flow), 2)         AS total_net_cash_flow,
# MAGIC     ROUND(AVG(payroll_pct_of_revenue), 1) AS avg_payroll_pct_of_revenue,
# MAGIC     ROUND(
# MAGIC         (SUM(cash_revenue) - LAG(SUM(cash_revenue)) OVER (ORDER BY calendar_year))
# MAGIC         / NULLIF(LAG(SUM(cash_revenue)) OVER (ORDER BY calendar_year), 0) * 100, 1
# MAGIC     ) AS revenue_growth_pct_vs_prior_year
# MAGIC FROM cash_flow_project.cash_flow_gold.fact_monthly_summary
# MAGIC GROUP BY calendar_year
# MAGIC ORDER BY calendar_year

# COMMAND ----------

# MAGIC %sql
# MAGIC -- "How many months could I survive if revenue stopped today" - a classic owner question
# MAGIC CREATE OR REPLACE VIEW cash_flow_project.cash_flow_gold.vw_cash_runway AS
# MAGIC SELECT
# MAGIC     month_key,
# MAGIC     ROUND(month_end_balance, 2) AS closing_balance,
# MAGIC     ROUND(avg_3m_expenses, 2)   AS avg_3m_expenses,
# MAGIC     ROUND(
# MAGIC         CASE WHEN avg_3m_expenses > 0
# MAGIC              THEN month_end_balance / avg_3m_expenses
# MAGIC              ELSE NULL END, 1
# MAGIC     ) AS runway_months
# MAGIC FROM cash_flow_project.cash_flow_gold.fact_monthly_summary
# MAGIC ORDER BY month_key

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expense Deep-Dive page

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monthly spend by category, plus each category's share of total expenses
# MAGIC CREATE OR REPLACE VIEW cash_flow_project.cash_flow_gold.vw_expense_breakdown AS
# MAGIC SELECT
# MAGIC     month_key,
# MAGIC     ROUND(cogs_expense, 2)                              AS cogs,
# MAGIC     ROUND(operating_expense, 2)                         AS operating_expense,
# MAGIC     ROUND(total_payroll, 2)                             AS payroll,
# MAGIC     ROUND(total_cc_spend, 2)                            AS credit_card_spend,
# MAGIC     ROUND(total_expenses, 2)                            AS total_expenses,
# MAGIC     ROUND(total_payroll   / NULLIF(total_expenses,0) * 100, 1) AS payroll_pct,
# MAGIC     ROUND(cogs_expense    / NULLIF(total_expenses,0) * 100, 1) AS cogs_pct,
# MAGIC     ROUND(total_cc_spend  / NULLIF(total_expenses,0) * 100, 1) AS credit_card_pct,
# MAGIC     ROUND(operating_expense / NULLIF(total_expenses,0) * 100, 1) AS operating_pct
# MAGIC FROM cash_flow_project.cash_flow_gold.fact_monthly_summary
# MAGIC ORDER BY month_key

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Credit card spend by vendor - which vendor is costing the most over time
# MAGIC CREATE OR REPLACE VIEW cash_flow_project.cash_flow_gold.vw_credit_card_vendor_spend AS
# MAGIC SELECT
# MAGIC     date_format(date, 'yyyy-MM') AS month_key,
# MAGIC     description                  AS vendor,
# MAGIC     category_name,
# MAGIC     ROUND(SUM(amount), 2)        AS total_spend,
# MAGIC     COUNT(*)                     AS num_transactions
# MAGIC FROM cash_flow_project.cash_flow_gold.fact_transactions
# MAGIC WHERE account_name = 'Credit Card' AND type = 'Debit' AND category_name != 'Internal Transfer'
# MAGIC GROUP BY date_format(date, 'yyyy-MM'), description, category_name
# MAGIC ORDER BY month_key, total_spend DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Payroll by employee/role, with each person's pay as % of that month's revenue
# MAGIC CREATE OR REPLACE VIEW cash_flow_project.cash_flow_gold.vw_payroll_analysis AS
# MAGIC SELECT
# MAGIC     p.month_key,
# MAGIC     p.employee_name,
# MAGIC     p.role,
# MAGIC     p.type AS pay_type,
# MAGIC     p.total_paid,
# MAGIC     m.cash_revenue,
# MAGIC     ROUND(CASE WHEN m.cash_revenue > 0 THEN p.total_paid / m.cash_revenue * 100 ELSE 0 END, 1)
# MAGIC         AS pct_of_monthly_revenue,
# MAGIC     m.shortage_flag
# MAGIC FROM cash_flow_project.cash_flow_gold.fact_payroll p
# MAGIC LEFT JOIN cash_flow_project.cash_flow_gold.fact_monthly_summary m
# MAGIC     ON p.month_key = m.month_key
# MAGIC ORDER BY p.month_key, p.total_paid DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales & Products page

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW cash_flow_project.cash_flow_gold.vw_sales_product_performance AS
# MAGIC SELECT
# MAGIC     month_key,
# MAGIC     product_category,
# MAGIC     product_type,
# MAGIC     SUM(revenue)              AS total_revenue,
# MAGIC     SUM(transaction_qty)      AS total_units_sold,
# MAGIC     COUNT(transaction_id)     AS num_transactions,
# MAGIC     ROUND(AVG(unit_price), 2) AS avg_unit_price,
# MAGIC     hour,
# MAGIC     CASE
# MAGIC         WHEN hour BETWEEN 6  AND 11 THEN 'Morning (6-11am)'
# MAGIC         WHEN hour BETWEEN 12 AND 14 THEN 'Lunch (12-2pm)'
# MAGIC         WHEN hour BETWEEN 15 AND 17 THEN 'Afternoon (3-5pm)'
# MAGIC         WHEN hour BETWEEN 18 AND 21 THEN 'Evening (6-9pm)'
# MAGIC         ELSE 'Other'
# MAGIC     END AS time_of_day
# MAGIC FROM cash_flow_project.cash_flow_gold.fact_sales
# MAGIC GROUP BY month_key, product_category, product_type, hour
# MAGIC ORDER BY month_key, total_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI Recommendations page

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Feeds the Groq/Llama AI layer. STARTUP_PERIOD replaces the old false-alarm
# MAGIC -- behavior for a business's first 3 months (see 03_Gold_Layer for why).
# MAGIC CREATE OR REPLACE VIEW cash_flow_project.cash_flow_gold.vw_shortage_detection AS
# MAGIC SELECT
# MAGIC     month_key,
# MAGIC     shortage_flag,
# MAGIC     shortage_severity,
# MAGIC     ROUND(cash_revenue, 2)      AS monthly_income,
# MAGIC     ROUND(total_expenses, 2)    AS monthly_expenses,
# MAGIC     ROUND(net_cash_flow, 2)     AS net_cash_flow,
# MAGIC     ROUND(month_end_balance, 2) AS closing_balance,
# MAGIC     ROUND(avg_3m_revenue, 2)    AS avg_3m_income,
# MAGIC     ROUND(avg_3m_expenses, 2)   AS avg_3m_expenses,
# MAGIC     ROUND(
# MAGIC         CASE WHEN avg_3m_revenue > 0
# MAGIC              THEN (cash_revenue - avg_3m_revenue) / avg_3m_revenue * 100
# MAGIC              ELSE NULL END, 1)  AS income_vs_avg_pct,
# MAGIC     ROUND(
# MAGIC         CASE WHEN avg_3m_expenses > 0
# MAGIC              THEN (total_expenses - avg_3m_expenses) / avg_3m_expenses * 100
# MAGIC              ELSE NULL END, 1)  AS expense_vs_avg_pct,
# MAGIC     needs_ai_suggestion,
# MAGIC     CASE shortage_flag
# MAGIC         WHEN 'CRITICAL'       THEN 'Net cash flow is negative - spent more than earned'
# MAGIC         WHEN 'EXPENSE_SPIKE'  THEN 'Expenses are 20%+ above the 3-month average'
# MAGIC         WHEN 'INCOME_DROP'    THEN 'Revenue dropped 20%+ below the 3-month average'
# MAGIC         WHEN 'WARNING'        THEN 'Balance below 1.5x monthly expenses - low buffer'
# MAGIC         WHEN 'STARTUP_PERIOD' THEN 'Business is in its first 3 months - not enough history for a reliable baseline yet'
# MAGIC         ELSE 'No issues detected'
# MAGIC     END AS flag_explanation
# MAGIC FROM cash_flow_project.cash_flow_gold.fact_monthly_summary
# MAGIC ORDER BY shortage_severity DESC, month_key

# COMMAND ----------

# MAGIC %md
# MAGIC ## One-stop executive summary
# MAGIC A single row per month with the handful of numbers an owner checks first.
# MAGIC Good default landing view for the Executive Overview page.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW cash_flow_project.cash_flow_gold.vw_business_kpi_summary AS
# MAGIC SELECT
# MAGIC     m.month_key,
# MAGIC     m.quarter,
# MAGIC     ROUND(m.cash_revenue, 2)              AS revenue,
# MAGIC     ROUND(m.total_expenses, 2)            AS expenses,
# MAGIC     ROUND(m.net_cash_flow, 2)             AS net_cash_flow,
# MAGIC     ROUND(m.month_end_balance, 2)         AS closing_balance,
# MAGIC     ROUND(m.payroll_pct_of_revenue, 1)    AS payroll_pct_of_revenue,
# MAGIC     ROUND(CASE WHEN m.avg_3m_expenses > 0
# MAGIC                THEN m.month_end_balance / m.avg_3m_expenses ELSE NULL END, 1) AS cash_runway_months,
# MAGIC     p.pos_gross_revenue,
# MAGIC     m.shortage_flag,
# MAGIC     m.needs_ai_suggestion
# MAGIC FROM cash_flow_project.cash_flow_gold.fact_monthly_summary m
# MAGIC LEFT JOIN (SELECT month_key, SUM(revenue) AS pos_gross_revenue
# MAGIC            FROM cash_flow_project.cash_flow_gold.fact_sales GROUP BY month_key) p
# MAGIC     ON m.month_key = p.month_key
# MAGIC ORDER BY m.month_key
