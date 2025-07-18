# 🏷️ Adidas Global Sales

This project demonstrates an end-to-end ETL pipeline for Adidas global sales data using **Apache Airflow**, **PostgreSQL**, and **Elasticsearch**, with interactive dashboards built in **Kibana**.

---

## 📁 Repository Structure
```
1. visualisasi_kibana folder            : Folder berisi screenshot visualisasi dashboard di Kibana
2. adidas_sales_etl_dag.py              : Berisi definisi DAG yang digunakan untuk mengelola alur kerja ETL (Extract, Transform, Load)
3. dataset_raw.csv                      : Dataset mentah yang berasal dari kaggle
4. dataset_clean.csv                    : Dataset hasil cleaning dari proses DAG
5. query_ddl.sql                        : Skrip SQL untuk membuat dan mendefinisikan tabel dalam basis data PostgreSQL
```
---

## 📌 Overview

This project aims to analyze Adidas’ global sales data from 2020 to 2021.  
The main focus is to build an automated ETL pipeline to extract data from PostgreSQL, clean and transform it, then index it into Elasticsearch for interactive analysis in Kibana.  
Insights extracted include regional sales trends, product performance, and customer behavior across sales methods and geographies.

---

## 📦 Dataset

- **Source**: [Kaggle - Adidas Sales Dataset](https://www.kaggle.com/datasets/heemalichaudhari/adidas-sales-dataset)  
- **Time Range**: 2020–2021  
- **Condition**:  
  - No missing values  
  - No duplicates  
  - Some formatting issues (`$`, `,`, `%`) in numerical columns  

---

## 🧠 Objectives

- Build a reliable ETL pipeline with Apache Airflow  
- Perform data cleaning and transformation with pandas  
- Store raw and clean datasets for transparency  
- Index cleaned data into Elasticsearch  
- Visualize KPIs and trends using Kibana  

---

## 🔁 ETL Pipeline Steps

**1. Data Ingestion**  
Airflow pulls sales data from a PostgreSQL table and saves it as a raw CSV (`dataset_raw.csv`).

**2. Data Cleaning & Preprocessing**  
Remove symbols like `$`, `,`, and `%`, convert data types, and rename columns to `snake_case`.

**3. Data Indexing**  
The clean dataset is indexed into Elasticsearch (`adidas_sales` index) for real-time querying and dashboarding in Kibana.