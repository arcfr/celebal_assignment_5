# Celebal week 5 assignment - Employee Data Cleaning and Transformation and applying filtering and aggregation using PySpark

## Overview

This project focuses on cleaning and transforming a raw employee dataset using **PySpark**. The goal was to process inconsistent and messy data into a structured format suitable for analysis and further processing.

The cleaning pipeline handles common real-world data issues such as duplicate records, missing values, invalid entries, inconsistent formatting, datatype conversion, and feature extraction.

---

## Features Implemented

### Data Cleaning

* Removed duplicate records
* Replaced empty strings with null values
* Handled invalid categorical values such as:

  * `null`
  * `nan`
  * `unknown`
  * empty strings

### Data Transformation

* Standardized text columns using trimming and lowercase formatting
* Converted salary column into numeric datatype
* Cleaned and standardized multiple date formats
* Split combined fields into separate meaningful columns (Department and Region)

### Missing Value Handling

* Applied **mean imputation** for numerical columns:

  * Age
  * Salary

* Applied **mode imputation** for categorical columns:

  * Status
  * Performance Score
  * Department
  * Region

### Reporting

* Generated a cleaning summary report showing:

  * Number of duplicates removed
  * Invalid values cleaned
  * Missing values imputed

---

## Technologies Used

* **Python**
* **PySpark**
* **Apache Spark DataFrames**

---

## Project Structure

## Project Structure

```text
project/
│── data/
│   └── Messy_Employee_dataset.csv
│
│── outputs/
│   └── Week 5 Assignment Outputs.docx
│
│── scripts/
│   ├── data_cleaning.py
│   ├── data_ingestion.py
│   ├── data_inspection.py
│   ├── data_queries.py
│   └── run_pipeline.py
│
│── .gitignore
│── README.md
│── requirements.txt
```

### File Description

* **data_ingestion.py** → Loads the dataset into PySpark for processing.
* **data_inspection.py** → Performs dataset inspection and initial analysis.
* **data_cleaning.py** → Handles duplicate removal, null handling, standardization, datatype conversion, and imputation.
* **data_queries.py** → Contains Spark queries for analysis and insights.
* **run_pipeline.py** → Main pipeline script to execute the workflow.
* **outputs/** → Stores generated assignment outputs.
* **data/** → Contains the raw employee dataset.

```

Author

Archit Sahay
