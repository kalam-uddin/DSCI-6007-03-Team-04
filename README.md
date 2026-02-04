# Healthcare Cost Transparency Pipeline

This project implements a scalable, serverless ETL pipeline on AWS to provide cost transparency for healthcare services. The pipeline is designed to automatically process raw CSV data, transform it using AWS Glue, and make it queryable through Amazon Athena for visualization via Power BI.

---

## 📂 Repository Structure

```
├── lambda_function.py                # AWS Lambda function to trigger Glue job on S3 upload
├── etl_glue_invoked_by_lambda2.py   # AWS Glue script to transform CSV to Parquet
├── README.md                         # Project documentation (this file)
```

---

## 🔁 Data Flow Architecture

```
1. Upload raw CSV to S3 (term-project-source)
2. Lambda is triggered on file upload
3. Lambda invokes the Glue job
4. Glue transforms data and stores Parquet files in S3 (term-project-target/glue_transformed_data/)
5. AWS Glue Crawler updates Athena table
6. Athena serves the data to Power BI / Dash
```

---

## 🚀 Components Used

- **AWS S3** – Raw data storage (source) & transformed data storage (target)
- **AWS Lambda** – Event-based trigger to start the ETL job
- **AWS Glue** – Serverless ETL to clean and convert CSV to Parquet
- **AWS Glue Crawler** – Schema discovery and table cataloging
- **Amazon Athena** – SQL querying over Parquet data
- **Power BI** – Visualization layer

---

## 🧠 Use Case

This project simulates a real-world healthcare use case:
> When new cost data (e.g., procedures, providers, patient copays) is uploaded to the system, it is instantly processed and made queryable for dashboards and reports that support healthcare transparency.

---

## ⚙️ Setup Instructions

1. **Upload `lambda_function.py` to an AWS Lambda function**
    - Configure trigger on `term-project-source` S3 bucket (ObjectCreated)

2. **Create a Glue Job using `etl_glue_invoked_by_lambda2.py`**
    - Accepts `--S3_SOURCE_PATH` and `--S3_DESTINATION_PATH` as arguments

3. **Grant necessary IAM roles** to Lambda and Glue

4. **Set up Glue Crawler**
    - Point to `term-project-target` bucket
    - Create table in Athena database (e.g., `term_project`)

5. **Query (Athena)**: Data is made available via SQL queries in Athena.
6. **Visualization**: The results can be consumed in Power BI or a local Dash app to present healthcare cost insights.

**Demo**: For Demo refer to the following Google Drive Link (https://drive.google.com/file/d/1oBLGYqVzLiv6UP09KzykKW8zQ_TIahVK/view?usp=sharing)
