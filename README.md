# SQL Data Warehouse Project (Bronze–Silver–Gold Architecture)

This repository contains an end-to-end **SQL Server Data Warehouse project** designed using the **Medallion Architecture (Bronze, Silver, Gold)** approach.  
The project demonstrates how raw data is ingested, cleaned, transformed, and modeled into business-ready datasets for analytics and reporting.

This project is built as a **portfolio project** to showcase practical skills in:
- SQL Server
- Data Engineering
- ETL Development
- Data Modeling
- Analytics-ready design

---

## 🏗️ Data Architecture

The data warehouse follows a **three-layer architecture**:

- **Bronze Layer** – Raw data ingestion (as-is)
- **Silver Layer** – Data cleansing, standardization, and enrichment
- **Gold Layer** – Business-ready fact and dimension tables
- 
---

## 📌 Project Overview

This project covers the complete lifecycle of a data warehouse:

1. **Data Ingestion**
   - Source data from CRM and ERP systems provided as CSV files
   - Loaded into SQL Server using BULK INSERT

2. **Data Transformation**
   - Cleaning and standardization in the Silver layer
   - Handling nulls, invalid values, duplicates, and inconsistent formats

3. **Data Modeling**
   - Star schema design in the Gold layer
   - Creation of fact and dimension tables for analytics

4. **Analytics Readiness**
   - Optimized tables and views for reporting and ad-hoc SQL queries

---

## 🧱 Medallion Architecture Details

### 🥉 Bronze Layer
- Stores raw data exactly as received from source systems
- No transformations applied
- Acts as a historical and audit layer

### 🥈 Silver Layer
- Cleans and standardizes Bronze data
- Applies business rules and data quality checks
- Produces trusted, analytics-ready datasets

### 🥇 Gold Layer
- Contains business-focused tables
- Implements fact and dimension models
- Designed for reporting, dashboards, and analytics

---

## 🌟 About Me

Hi! I’m a Computer Engineering student with a strong interest in **Data Analytics and Data Engineering**.  
I enjoy working with SQL, data modeling, and building end-to-end data pipelines.  
This project reflects my hands-on learning and my goal of building reliable, analytics-ready data systems.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge\&logo=linkedin\&logoColor=white)](https://www.linkedin.com/in/rohit-devshatwar-178249296)  
[![LeetCode](https://img.shields.io/badge/LeetCode-FFA116?style=for-the-badge\&logo=leetcode\&logoColor=black)](https://leetcode.com/u/Rohit_Devshatwar/)  
[![GeeksforGeeks](https://img.shields.io/badge/GeeksforGeeks-2F8D46?style=for-the-badge\&logo=geeksforgeeks\&logoColor=white)](https://www.geeksforgeeks.org/profile/devshatwxqs5)
