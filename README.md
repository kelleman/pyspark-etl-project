# UFO Sightings ETL & Analytics Pipeline

An end-to-end data engineering project that extracts UFO sighting data from a CSV file, cleans and transforms it using **PySpark**, loads the processed data into **PostgreSQL**, performs analytical queries using **SQL**, and visualizes the results with **Metabase**.

The project demonstrates a complete ETL workflow:

**CSV → PySpark → PostgreSQL → SQL → Metabase Dashboard**

---

## 📌 Project Overview

The goal of this project is to build a practical ETL pipeline using a real-world UFO sightings dataset.

The raw dataset contains information about reported UFO sightings, including:

* City
* State
* Reported shape
* Reported colors
* Date and time of sighting

The raw data contains missing values, duplicate records, inconsistent column names, and timestamp information that requires transformation before the data can be used effectively for analysis.

The project processes the raw dataset with PySpark and loads the resulting dataset into PostgreSQL, where SQL is used for analytical exploration. Metabase is then connected to PostgreSQL to create interactive visualizations and a dashboard.

---

## 🏗️ Data Pipeline Architecture

```text
                    ┌─────────────────┐
                    │   Raw CSV File  │
                    │    ufo.csv      │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │     PySpark     │
                    │                 │
                    │   Extract       │
                    │   Clean         │
                    │   Transform     │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   PostgreSQL    │
                    │                 │
                    │ ufo_sightings   │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │      SQL       │
                    │    Analysis    │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │    Metabase     │
                    │                 │
                    │ Visualizations  │
                    │   Dashboard     │
                    └─────────────────┘
```

---

# 🛠️ Technologies Used

| Technology | Purpose                                        |
| ---------- | ---------------------------------------------- |
| Python     | Main programming language                      |
| PySpark    | Data extraction, cleaning and transformation   |
| PostgreSQL | Relational database for storing processed data |
| SQL        | Data analysis and querying                     |
| Docker     | Running PostgreSQL and Metabase containers     |
| Metabase   | Data visualization and dashboard               |
| Git        | Version control                                |
| GitHub     | Source code repository                         |

---

# 📂 Project Structure

```text
pyspark-etl-project/
│
├── data/
│   └── raw/
│       └── ufo.csv
│
├── src/
│   ├── spark_session.py
│   ├── extract.py
│   ├── transform.py
│   └── load.py
│
├── main.py
├── requirements.txt
└── README.md
```

---

# 📊 Dataset

The project uses a UFO sightings dataset containing historical reports of unidentified flying objects.

The original dataset contains the following main fields:

```text
City
Colors Reported
Shape Reported
State
Time
```

The dataset contains approximately **80,000 records**.

After cleaning and removing duplicate records, the final dataset contains:

```text
Rows: 79,963
Columns: 12
```

---

# 🔄 ETL Process

## 1. Extract

The raw UFO dataset is stored as:

```text
data/raw/ufo.csv
```

PySpark reads the CSV file into a Spark DataFrame.

The extraction process is handled by:

```text
src/extract.py
```

The data is initially loaded with the original column names and data types.

---

# 🧹 2. Data Cleaning

The cleaning stage was implemented using PySpark.

Several data-quality checks were performed before transformation.

### Schema Inspection

The schema was inspected to understand:

* Column names
* Data types
* Structure of the dataset

### Basic Data Information

The pipeline checks:

* Number of rows
* Number of columns
* Column names
* Data types

### Data Preview

Sample records were displayed to understand the contents of the dataset.

### Descriptive Statistics

Basic descriptive statistics were generated to inspect the numerical characteristics of the dataset.

### NULL Value Detection

NULL values were checked across all columns.

Selected missing values were replaced with:

```text
UNKNOWN
```

This was applied to fields such as:

```text
City
Colors Reported
Shape Reported
```

### Duplicate Detection

The pipeline checks for duplicate records by comparing all columns.

Duplicate records are then removed using:

```python
df.dropDuplicates()
```

After cleaning, the dataset was verified again to ensure that the expected NULL values were no longer present.

---

# 🔧 3. Data Transformation

After cleaning, several transformations were applied.

## Standardizing Column Names

The original column names contained spaces and uppercase characters.

For example:

```text
Colors Reported
Shape Reported
```

were converted to:

```text
colors_reported
shape_reported
```

The final column naming convention uses lowercase letters and underscores.

---

## Converting Time to Timestamp

The original `time` field was stored as a string.

It was converted into a proper Spark timestamp.

Example:

```text
2010-01-20 19:30:00
```

This allows the timestamp to be used for additional time-based analysis.

---

## Extracting Date Information

The timestamp was used to create additional analytical fields:

```text
date
year
month
month_name
day_of_week
hour
```

For example:

```text
time:
2010-01-20 19:30:00

year:
2010

month:
1

month_name:
January

day_of_week:
Wednesday

hour:
19
```

This makes it possible to analyze the dataset across different time dimensions.

---

## Counting Reported Colors

A new column called:

```text
color_count
```

was created.

This represents the number of colors reported for each sighting.

For example:

```text
BLUE
```

becomes:

```text
color_count = 1
```

While:

```text
GREEN BLUE
```

becomes:

```text
color_count = 2
```

For records where the reported color is:

```text
UNKNOWN
```

the color count is:

```text
0
```

---

# 📋 Final Dataset Schema

After transformation, the final dataset contains **12 columns**:

```text
city
state
shape_reported
colors_reported
color_count
time
date
year
month
month_name
day_of_week
hour
```

The final DataFrame contains:

```text
79,963 rows
12 columns
```

A final verification step checks:

* Row count
* Column count
* Schema
* NULL values

The final verification confirmed that the final dataset contained **0 NULL values** across the selected columns.

---

# 🗄️ 4. Load — PostgreSQL

The transformed Spark DataFrame is loaded into PostgreSQL using the Spark JDBC connector.

### PostgreSQL Database

```text
Database: ufo_db
```

### PostgreSQL Table

```text
ufo_sightings
```

The PostgreSQL database runs inside a Docker container.

The table contains the final transformed dataset.

---

## PostgreSQL Docker Container

PostgreSQL is run using Docker.

Example:

```bash
docker run -d \
  --name postgres-db \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=crypto_db \
  -p 5432:5432 \
  -v postgres_data:/var/lib/postgresql/data \
  postgres:17
```

The UFO database was created separately:

```sql
CREATE DATABASE ufo_db;
```

---

# 🔌 PostgreSQL Connection

The Spark application connects to PostgreSQL through JDBC.

The connection uses:

```text
Host: localhost
Port: 5432
Database: ufo_db
User: postgres
Table: ufo_sightings
```

The PostgreSQL JDBC driver used by the Spark application is:

```text
org.postgresql:postgresql:42.7.8
```

---

# 🔎 5. SQL Analysis

After loading the transformed data into PostgreSQL, SQL was used to analyze the dataset.

The analysis stage is separate from the PySpark transformation stage.

This allows the project to demonstrate both:

* PySpark data engineering
* SQL data analysis

---

## Sightings by State

```sql
SELECT
    state,
    COUNT(*) AS sighting_count
FROM ufo_sightings
GROUP BY state
ORDER BY sighting_count DESC;
```

This identifies the states with the highest number of reported sightings.

---

## Most Common UFO Shapes

```sql
SELECT
    shape_reported,
    COUNT(*) AS sighting_count
FROM ufo_sightings
GROUP BY shape_reported
ORDER BY sighting_count DESC;
```

This identifies the most frequently reported UFO shapes.

---

## Sightings by Year

```sql
SELECT
    year,
    COUNT(*) AS sighting_count
FROM ufo_sightings
GROUP BY year
ORDER BY year;
```

This allows the number of reported sightings to be examined over time.

---

## Sightings by Month

```sql
SELECT
    month,
    month_name,
    COUNT(*) AS sighting_count
FROM ufo_sightings
GROUP BY month, month_name
ORDER BY month;
```

The numeric `month` field is used to maintain the correct chronological order:

```text
January
February
March
April
...
December
```

---

## Average Number of Reported Colors

```sql
SELECT
    AVG(color_count) AS average_colors_reported
FROM ufo_sightings;
```

This calculates the average number of colors reported per UFO sighting.

---

# 📈 6. Metabase Dashboard

Metabase was connected to the PostgreSQL database to visualize the processed data.

The dashboard contains five visualizations.

### 1. Total UFO Sightings

Displays the total number of records:

```text
79,963
```

### 2. UFO Sightings by State

A bar chart showing the number of reported sightings by state.

### 3. UFO Sightings by Shape

A bar chart showing the distribution of reported UFO shapes.

### 4. UFO Sightings by Year

A line chart showing how the number of reported sightings changes over time.

### 5. UFO Sightings by Month

A bar chart showing the distribution of sightings across January through December.

---

# 🐳 Docker Setup

Docker is used to run the database and visualization services.

The main containers used in the project are:

```text
postgres-db
metabase
```

A Docker network was created so that Metabase can communicate with PostgreSQL using the PostgreSQL container name.

Example:

```bash
docker network create etl-network
```

PostgreSQL was connected to the network:

```bash
docker network connect etl-network postgres-db
```

Metabase was then started on the same network.

---

# 🚀 Running the Project

## Prerequisites

Before running the project, install:

* Python
* Java JDK 17
* PySpark
* Docker
* PostgreSQL JDBC driver through Spark's package configuration

---

## 1. Clone the Repository

```bash
git clone git@github.com:kelleman/pyspark-etl-project.git
```

Move into the project directory:

```bash
cd pyspark-etl-project
```

---

## 2. Install Python Dependencies

Create/activate your Python environment if you are using one.

Then install the project dependencies:

```bash
pip install -r requirements.txt
```

---

## 3. Start PostgreSQL

Start the PostgreSQL Docker container:

```bash
docker start postgres-db
```

If the container has not been created yet, create it using the Docker command shown earlier in this README.

---

## 4. Start Metabase

Start the Metabase container:

```bash
docker start metabase
```

Metabase will be available at:

```text
http://localhost:3000
```

---

## 5. Run the ETL Pipeline

From the project root:

```bash
python main.py
```

The pipeline will:

1. Create a Spark session
2. Read the raw CSV file
3. Inspect the data
4. Check NULL values
5. Check duplicate records
6. Clean the data
7. Remove duplicates
8. Standardize column names
9. Convert timestamps
10. Extract date/time information
11. Calculate reported color counts
12. Select the final columns
13. Verify the final dataset
14. Load the data into PostgreSQL

---

# 🔍 Verifying the PostgreSQL Data

You can connect to the PostgreSQL database using:

```bash
docker exec -it postgres-db psql -U postgres -d ufo_db
```

List the tables:

```sql
\dt
```

Inspect the UFO table:

```sql
\d ufo_sightings
```

Count the records:

```sql
SELECT COUNT(*)
FROM ufo_sightings;
```

Expected result:

```text
79963
```

Exit PostgreSQL:

```sql
\q
```

---

# 📚 Key Data Engineering Concepts Demonstrated

This project demonstrates several important data engineering concepts.

### ETL

The project implements:

```text
Extract → Transform → Load
```

### Distributed Data Processing

PySpark is used to process the dataset using Spark DataFrames.

### Data Cleaning

The pipeline handles:

* NULL values
* Duplicate records
* Column-name standardization
* Data type conversion

### Data Transformation

The project creates analytical fields from the original data.

### Relational Databases

Processed data is stored in PostgreSQL.

### JDBC

Spark communicates with PostgreSQL through JDBC.

### SQL

SQL is used to aggregate and analyze the processed dataset.

### Data Visualization

Metabase converts analytical results into charts and a dashboard.

### Docker

Docker provides reproducible environments for PostgreSQL and Metabase.

### Version Control

Git and GitHub are used to track and publish the project source code.

---

# 🎯 Project Objectives

The main objectives of this project were to:

* Build an end-to-end ETL pipeline
* Learn how to process data with PySpark
* Practice data cleaning and transformation
* Work with PostgreSQL
* Load Spark DataFrames into a relational database
* Write SQL analytical queries
* Build a dashboard with Metabase
* Practice Docker-based development
* Build a practical data engineering portfolio project

---

# 📌 Project Outcome

The completed pipeline successfully processes the UFO dataset and produces a cleaned dataset containing:

```text
79,963 records
12 columns
0 NULL values
```

The processed data is stored in PostgreSQL and made available for SQL analysis and Metabase visualization.

The final dashboard provides an overview of:

* Total UFO sightings
* Geographic distribution
* Reported UFO shapes
* Yearly trends
* Monthly distribution

---

# 🔮 Future Improvements

The current project focuses on building a simple and understandable ETL pipeline.

Possible future improvements include:

* Add automated ETL scheduling with Apache Airflow
* Add data-quality checks
* Add logging instead of relying primarily on console output
* Add automated tests
* Add incremental loading instead of overwriting the PostgreSQL table
* Add PostgreSQL indexes for analytical queries
* Containerize the complete application
* Add more advanced SQL analysis
* Add additional Metabase dashboard filters
* Add CI/CD with GitHub Actions
* Deploy the pipeline to a cloud environment

These improvements are intentionally outside the scope of the current version of the project.

---

# 🧠 What I Learned

Through this project, I gained practical experience with:

* PySpark DataFrames
* Spark transformations
* Data cleaning
* Handling NULL values
* Removing duplicate records
* Timestamp conversion
* Date/time extraction
* Column standardization
* Aggregation concepts
* PostgreSQL
* JDBC connections
* SQL aggregation
* Docker containers
* Metabase
* Data visualization
* Git and GitHub
* Building an end-to-end ETL workflow

---

# 👤 Author

**Godfrey Atser**

Data Engineering / Data Analytics Entry Level

GitHub:

```text
https://github.com/kelleman/pyspark-etl-project
```

---

# 📄 License

This project is intended primarily for educational and portfolio purposes.


