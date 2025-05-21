Project title : E-commerce Sales Data Analysis using Apache Spark

I focuses on analyzing an E-commerce sales dataset using Apache Spark with PySpark and Spark SQL. The goal is to derive meaningful business insights from raw transactional data stored in CSV format.

Key Objectives:

1. Spark Session Initialization:
A Spark session is initialized with Hive support to leverage Spark SQL capabilities for data processing.

2. Data Loading and Exploration:
The dataset, ecommerce_sales.csv, is loaded into a Spark DataFrame with schema inference enabled. Initial data exploration includes printing sample rows to understand the structure.

3. High-Value Order Filtering:
A temporary view sales_data is created to run SQL queries. Orders are filtered where the price exceeds ₹1000 and the quantity is 2 or more, identifying high-value transactions.

4. Sales Aggregation by Category:
Using SQL queries, sales data is aggregated based on product categories to compute:

Total revenue (SUM(quantity * price))

Average price per item (AVG(price))

Total number of orders (COUNT(order_id))

Result Export:
The high-value filtered orders and the aggregated category-wise sales summary are exported to CSV files with headers:

high_value_orders.csv

category_sales_summary.csv

Tools & Technologies Used:

1. Apache Spark

2. PySpark

3. Spark SQL

4. CSV file handling

5. Jupyter Notebook for interactive analysis

Outcome:
This project demonstrates how big data technologies like Apache Spark can be utilized for efficient data processing, complex querying, and summarization of large-scale E-commerce data to support data-driven business decisions.
