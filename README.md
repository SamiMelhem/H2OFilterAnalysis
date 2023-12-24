# H2O Filter Analysis

## Overview
H2O Filter Analysis is my internship project that focuses on analyzing water constituent data to provide filtration advice based on government guidelines. This application takes input data about water quality, processes it through statistical analysis using Python and its libraries, and outputs recommendations on the filtration frequency and methods. The project integrates data science techniques with environmental science knowledge, ensuring decisions are informed, data-driven, and aligned with health and safety standards.

## Features
- Data analysis based on input water constituent data for statistical analysis and decision-making.
- Reads in filtered datasets for trend analysis.
- Outputs data for assessing and proposing actions based on stats and counts.

## Technologies Used
This project is built using Python and various libraries:
- _Pandas_ for data manipulation
- _Seaborn_ and _Matplotlib_ for data visualization
- _Lifelines_ for survival analysis
- _PymannKendall_ for trend analysis

## Installation
Before running the project, ensure you have the following packages installed:
- pandas
- seaborn
- matplotlib
- numpy
- lifelines
- pymannkendall
- openpyxl

You can install these packages using pip:
```bash
pip install pandas seaborn matplotlib numpy lifelines pymannkendall openpyxl
```

## Usage
To run the H2O Filter Analysis project:

1. Navigate to the `scripts` directory.
2. Run the `Complete_Script` file, which orchestrates the data analysis and filtration advice process:
```bash
python Complete_Script.py
```
