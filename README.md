# E-commerce Medallion Architecture Pipeline

## 📌 Project Overview
This project demonstrates how to build a complete **data engineering pipeline** on **Databricks** using the **Medallion Architecture** (Bronze → Silver → Gold).  
The pipeline processes raw e-commerce data ncluding **products, customers, orders, brands and categories** and transforms it into **analytics-ready fact and dimension tables** stored in Delta Lake.

The final Gold layer powers **interactive dashboards** built directly in Databricks to analyze sales trends, customer behavior, product performance, and operational metrics.

---

## 🎯 Motivation
As a data engineer working with Spark and data pipelines, I wanted hands-on experience with:

- The **Databricks Lakehouse platform**
- Understanding **catalogs, schemas, volumes, workspaces**, and the Databricks UI
- Implementing **Delta Lake** features such as ACID transactions, time travel, schema evolution, partitioning, and optimization
- Designing **Medallion Architecture** pipelines (Bronze/Silver/Gold)
- Building **fact and dimension models** for analytics
- Creating **dashboards** using Databricks built-in visualization tools

This project serves as a practical end-to-end learning experience replicating real-world data engineering workflows.

---

## 🏗️ Architecture

### 1. **Bronze Layer – Raw Data Ingestion**
- Ingest raw CSV data for:
  - Customers  
  - Products  
  - Categories
  - Brands
  - Orders  
- Apply minimal transformations  
- Store as **Delta tables** in the Bronze layer  

### 2. **Silver Layer – Cleaned & Standardized**
- Clean missing values  
- Standardize data types  
- Remove duplicates  
- Apply basic business rules  

### 3. **Gold Layer – Analytics-Ready**
- Build **Dimension Tables**:
  - `dim_customers`
  - `dim_products`
  - `dim_date`
  - `dim_categories`

- Build **Fact Tables**:
  - `fact_transactions`

---

## 📊 Dashboards & Analytics
Using Databricks' built-in visualization tools, the project includes:

- Sales trends over time  
- Sales per categories

---

## 🛠️ Technologies Used

| Category | Tools |
|---------|-------|
| **Platform** | Databricks Community Edition |
| **Processing Engine** | Apache Spark (PySpark) |
| **Storage** | Delta Lake |
| **Architecture** | Medallion Architecture (Bronze/Silver/Gold) |
| **Modeling** | Star Schema (Fact & Dimension Tables) |
| **Visualization** | Databricks Dashboards |
| **Data Format** | CSV / Delta |
| **Concepts** | Partitioning, Schema Enforcement, Optimization, Catalogs, Schemas, Volumes |

---


