# Apache_Airflow
 # ETL Toll Data Pipeline

This project is an ETL (Extract, Transform, Load) pipeline for processing toll data using Apache Airflow. The pipeline is designed to:

*  Unzip the toll data archive.
*  Extract data from CSV, TSV, and fixed-width files.
*  Consolidate the extracted data.
*  Transform the consolidated data.
  
DAG Structure
The DAG (ETL_toll_data) follows these steps:

*  Unzip Data: Unzips the toll data archive.
*  Extract Data from CSV: Extracts specific columns from a CSV file.
*  Extract Data from TSV: Extracts specific columns from a TSV file.
*  Extract Data from Fixed Width File: Extracts specific columns from a fixed-width text file.
*  Consolidate Data: Combines the extracted data from the CSV, TSV, and fixed-width files.
*  Transform Data: Transforms the consolidated data for further analysis.
  
Prerequisites
*  Apache Airflow installed and configured.
*  Pandas library installed.
*  The required data files should be present in the specified directories.
