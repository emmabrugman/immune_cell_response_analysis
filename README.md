# Immune Cell Response Analysis

 #### This repository contains an analysis pipeline for exploring immune cell population dynamics in a clinical trial setting. The goal of the pipeline is to help a drug development team understand how immune cell populations differ across several modalities.
---
## Project Structure

### Database Schema
The dataset is stored in a normalized SQLite relational database to ensure data integrity, reduce redundancy, and support scalable analytical queries.

The data is stored in a normalized SQLite database with *four tables*:

- **projects** (`project_id` PK)  
- **subjects** (`subject_id` PK, `project_id` FK → projects, age, sex)  
- **samples** (`sample_id` PK, `subject_id` FK → subjects, `project_id` FK → projects, condition, treatment, response, sample_type, time_from_treatment_start)  
- **cell_counts** (`id` PK, `sample_id` FK → samples, population, count, UNIQUE(sample_id, population))

Project-level, subject-level, and sample-level metadata are separated to avoid duplication when subjects have multiple samples or timepoints. Immune cell populations are stored in long format in the `cell_counts` table (one row per population per sample), which makes it easy to add new populations without modifying the schema.

Primary keys, foreign keys, and uniqueness constraints enforce consistency across tables. Indexes on commonly filtered fields such as condition, treatment, response, sample type, timepoint, and project support efficient querying as the dataset grows.

This design naturally scales to hundreds of projects and thousands of samples. Additional data types or analyses can be supported by adding new tables or new rows without restructuring existing tables.

### Repo Structure

    
    ├── data/
    │   └── cell-count.csv          # Input dataset
    ├── src/
    │   ├── db.py                   # Database schema + data loader
    │   └── analysis.py             # Analysis
    ├── results/
    │   ├── summary_table.csv
    │   ├── boxplot_responders.png
    │   ├── statistical_results.csv
    │   └── part4_summary.csv
    ├── cell_count.db               # SQLite database
    └── README.md
### Code structure

#### `src/db.py`

This file handles database setup and data ingestion. It defines the SQLite schema, creates all tables and indexes, and loads the contents of `cell-count.csv` into the database. 

#### `src/analysis.py`

This file contains all analysis logic and reporting:

- Generates a per-sample summary table with total cell counts and relative frequencies
- Compares responders vs non-responders for melanoma patients treated with miraclib using PBMC samples, including visualization and statistical testing
- Performs a baseline (time = 0) subset analysis for melanoma PBMC samples treated with miraclib

The database layer is kept separate from the analysis code so that queries are reproducible, easy to modify, and do not require reloading or reprocessing the raw CSV for each analysis.

---
## To run:

### Install dependencies
```bash
pip install pandas matplotlib seaborn scipy
```
### Initialize and load the database 
```bash
python src/db.py
```

### Compute the relative frequency of each immune cell population per sample
```bash
python src/analysis.py summary
```

### Compute responder vs non-responder analysis
```bash
python src/analysis.py responders
```

### Compute baseline analysis
```bash
python src/analysis.py baseline
```
### Outputs are written to results/:
```
results/summary_table.csv

results/boxplot_responders.png

results/statistical_results.csv

results/part4_summary.csv
```