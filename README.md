# PostgreSQL Data Hub

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/dorukalkan/pgdatahub/blob/master/LICENSE)
[![Python](https://img.shields.io/badge/python-3.6%2B-blue)](https://www.python.org/)
[![Maintenance](https://img.shields.io/badge/maintained-yes-green.svg)](https://github.com/dorukalkan/pgdatahub/graphs/commit-activity)

pgdatahub is a multi-format PostgreSQL data import tool that automates ETL operations by processing various file formats (CSV, JSON, Excel) and importing them into a PostgreSQL database.

## Overview

pgdatahub automates the process of importing data from different file formats into PostgreSQL databases. It handles the entire pipeline from detecting data files to creating properly structured database tables and importing the data. This tool is particularly useful for data analysts and engineers who need to quickly load multiple datasets into a PostgreSQL database without writing custom import scripts for each file format.

## Features

### File detection and processing
- **Automatic file detection**: Scans the current directory for CSV, JSON, and Excel files
- **Multi-format support**: Handles CSV, JSON, Excel (.xlsx, .xls, .xlsm, .xlsb, .odf, .ods, .odt)
- **Multi-sheet Excel support**: Creates separate tables for each sheet in Excel workbooks

### Data cleaning and transformation
- **Standardized naming**: Converts column names to database-friendly formats
- **Turkish character conversion**: Transliterates Turkish characters to Latin equivalents
- **SQL data type mapping**: Maps pandas data types to appropriate SQL data types

### Database creation
- **Automatic schema generation**: Creates database schemas based on data structure
- **Efficient data loading**: Uses PostgreSQL's COPY command for fast data insertion
- **Configurable connection**: Connect to any PostgreSQL server via configuration file

## Installation

### Prerequisites
- Python 3.6+
- PostgreSQL server (local or remote)
- pandas
- psycopg2
- openpyxl

### Steps
1. Clone the repository:
   ```
   git clone https://github.com/dorukalkan/pgdatahub.git
   cd pgdatahub
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Configuration

Database connection settings are stored in the `config.json` file. **Note: When you first clone this repository, you will only have `config.template.json` (not the actual config file).**

### Setting up configuration (first-time setup):

1. Create a new text file in the same folder as the project and name it `config.json`
2. Open `config.template.json` to view the template structure
3. Copy the contents from the template and paste them into your new `config.json` file
4. Replace the placeholder values with your actual PostgreSQL database credentials:
   ```json
   {
       "database": {
           "host": "localhost",
           "database": "your_database",
           "user": "your_username",
           "password": "your_password",
           "port": 5432
       }
   }
   ```
5. Save the file

    ### Using an environment variable (recommended) 💡
    For remote databases (for example, Supabase), it's recommended to set a `DATABASE_URL` environment variable instead of committing credentials to disk.
    - Example formatted connection string:
      ```
      postgresql+psycopg2://postgres:YOUR_PASSWORD@db.srwadirqletenulzzlrh.supabase.co:5432/postgres?sslmode=require
      ```
    - To set it in your shell:
      ```bash
      export DATABASE_URL='postgresql+psycopg2://postgres:YOUR_PASSWORD@db.srwadirqletenulzzlrh.supabase.co:5432/postgres?sslmode=require'
      ```
    - `src/config.py` will read `DATABASE_URL` if present and normalize the URL for SQLAlchemy.

    **Security note:** Never commit real passwords to your repository; use environment variables or a secrets manager. Do not include real credentials in `config.example.json` — keep it a template with placeholders, and rotate any credential immediately if it was exposed.

    **If a secret was exposed:**
    1. Rotate the credential immediately (e.g., reset the DB password in Supabase dashboard).
    2. Remove the secret from your local files and replace it with placeholders.
    3. If the secret was pushed to a remote, consider removing it from git history using tools like `git filter-repo` or [BFG Repo-Cleaner](https://rtyley.github.io/bfg-repo-cleaner/) (this rewrites history and requires a forced push and coordination with collaborators).
    4. After rotation, update any systems that used the old secret and confirm access works with the new credential.
1. Move your original data files to an "unprocessed_data" directory
2. Process each file and create appropriate dataframes
3. Clean and standardize column names and file names
4. Create database tables with appropriate schemas
5. Import all data into your PostgreSQL database
6. Move the processed CSV files to a "processed_data" directory

## Sample data

The repository includes sample datasets in the `sample_data` directory that demonstrate the features of pgdatahub:

- **Album-Records.json**: A JSON file demonstrating how JSON structures are converted to database tables
- **Customer Data & Info.csv**: CSV file showing how special characters and spaces in headers are handled
- **Product Sales & User Data.xlsx**: Excel file with multiple sheets, demonstrating how each sheet becomes a separate table

These files showcase key features including:
- File and column name standardization (spaces and special characters → underscores)
- Turkish character transliteration (ö, ç, ş, ğ, ü, ı → o, c, s, g, u, i)
- Multi-sheet Excel processing
- Type mapping from source formats to appropriate SQL data types

To try out pgdatahub with these sample files, simply copy them to the root directory and run the script.

## Logging

pgdatahub includes comprehensive logging that saves information about each run to a timestamped log file. The logs include details about file processing, data cleaning, and database operations, making it easier to troubleshoot any issues.

## Acknowledgements

This project was initially inspired by StrataScratch's [CSV File to Database Import Automation](https://github.com/Strata-Scratch/csv_to_db_automation) project. pgdatahub has been built on top of this project and has significantly expanded the core functionalities to include:

- Support for Excel file formats with multiple sheets 
- Support for JSON files
- Enhanced data cleaning with regex functions
- Turkish character transliteration
- Extensive error handling and logging
- Secure database configuration

You can check out [StrataScratch](https://www.stratascratch.com) for data science resources and watch their tutorials here:
- [Solve Data Science Tasks In Python](https://youtu.be/wqBFgaMgFQA?feature=shared)
- [Automating Your Data Science Tasks In Python](https://youtu.be/TDwy1lSjEZo?feature=shared)
- [Applying Software Engineering Principles To Your Data Science Tasks In Python](https://youtu.be/N0aHeKyNEto?feature=shared)

## Contact

You can reach me at [dorukalkan1.0@gmail.com](mailto:dorukalkan1.0@gmail.com) for any issues, questions, or suggestions.

## License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/dorukalkan/pgdatahub/blob/master/LICENSE) file for details.
