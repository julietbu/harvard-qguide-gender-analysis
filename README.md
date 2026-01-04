# harvard-qguide-gender-analysis
Replication files for a final project completed for GOV1372: Political Psychology at Harvard University, Fall 2025.

# Overview

This project examines whether students display gender bias against female lecturers in Harvard course evaluations and the relationship between Harvard students' gender bias and political ideology using Harvard course evaluations (QGuide data) for all courses offered in the Spring 2025 semester. 
I use R to estimate OLS regressions and compare average course and lecturer numerical scores for female and male lecturers. The repository contains all files necessary to reproduce the empirical analysis and results presented in the final project. 

# Data
- **Source:** QGuide 2025 Spring Data, scraped by Harvard student Jay Chooi [https://github.com/jeqcho/myharvard_qguide_scraper/tree/main/release/qguide]
- **Time period:** Spring 2025
- **Key variables:** _course_score_mean_ - average numerical score, between 1-5, given to a course; _lecturer_score_mean_ - average numerical score, between 1-5, given to a lecturer

The dataset used in this analysis is stored in `data/raw/`. The cleaned dataset is stored in `data/processed/`

# Code
- `code/qguide_analysis.Rmd`: Main R markdown file that runs the full analysis.
  This script cleans the data, generates all variables, and produces the regression output used in the paper.
- `code/codex_qguide_gender.py`: Python script, written with assistance from codex. This script scrapes lecturers' first names and labels lecturer gender for each course. 

  # Paper
- `paper/QGuide Analysis Final Paper.pdf`: Final written report describing the research
  question, data, methodology, results, and conclusions.

  ## Author

Juliet Bu, Harvard College 2027  
Harvard University  
Department of Government  
This project was completed as part of coursework and reflects
independent student work.
