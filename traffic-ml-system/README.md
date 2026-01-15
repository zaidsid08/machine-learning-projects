# Traffic Congestion & ETA Prediction System

## Overview

This project is an end-to-end **machine learning–based traffic congestion and ETA prediction system** designed to model real-world traffic behavior using historical data and time-aware features.

The system forecasts short-term traffic congestion based on recent traffic patterns and temporal context, then uses these predictions as a foundation for estimating expected travel time (ETA). The project emphasizes **clean data pipelines, time-series feature engineering, and ML system design**, rather than isolated notebook experiments.

The long-term goal is to approximate how modern navigation systems reason about traffic conditions and travel time under uncertainty.

---

## Problem Statement

Urban traffic congestion is highly time-dependent and influenced by recent traffic behavior, recurring temporal patterns, and contextual factors. Accurately forecasting congestion—and translating those forecasts into ETA estimates—requires more than static averages.

This project aims to:
- Predict short-term traffic congestion from historical observations
- Capture daily and weekly traffic patterns
- Provide a framework for converting congestion predictions into ETA estimates
- Serve as a foundation for route-based and context-aware traffic modeling

---

## Data Source

The initial dataset used in this project is sourced from Kaggle:

- **Dataset:** Traffic Prediction Dataset
- **Platform:** Kaggle
- **URL:** https://www.kaggle.com/datasets/fedesoriano/traffic-prediction-dataset

The dataset contains:
- Timestamped traffic observations
- Junction identifiers representing traffic locations
- Vehicle counts, which are used as a **proxy for traffic congestion**

Raw data is stored immutably in `data/raw/` and is treated as read-only.  
All transformations and cleaning steps are performed via code and saved separately in `data/processed/`.

---

## System Design

The project follows an industry-style machine learning pipeline:

### 1. Data Ingestion
- Raw datasets are stored in `data/raw/`
- Original files are never modified manually

### 2. Data Cleaning & Processing
- Timestamp parsing and validation
- Removal of invalid or missing observations
- Selection of relevant columns
- Sorting by location and time
- Cleaned outputs saved to `data/processed/`

### 3. Feature Engineering
- Rolling window features capturing recent traffic behavior
- Temporal features (hour of day, day of week)
- Designed to support future contextual features (e.g., weather)

### 4. Machine Learning
- Baseline and ML models for short-term congestion forecasting
- Time-aware train/test splits
- Evaluation using appropriate regression metrics

### 5. ETA Estimation (Planned)
- Mapping congestion predictions to estimated travel time
- Future integration with routing and road network data

---

## Project Structure

```text
traffic-ml-system/
├── data/
│   ├── raw/            # original datasets (read-only)
│   └── processed/      # cleaned and feature-ready data
├── models/             # trained models
├── notebooks/          # exploratory analysis
├── src/
│   ├── api/            # prediction API (planned)
│   ├── features/       # feature engineering
│   ├── ml/             # training and evaluation
│   ├── routing/        # ETA and routing logic (planned)
│   └── utils/          # shared utilities
├── ui/                 # user interface (planned)
├── requirements.txt
└── README.md
