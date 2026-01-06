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

## Dataset

The initial phase of the project uses a public traffic dataset containing:
- Timestamped traffic observations
- Junction identifiers representing traffic locations
- Vehicle counts as a proxy for congestion level

Traffic volume is treated as a **congestion signal** in early stages.  
The system is designed to later support:
- Speed-based targets
- Road-segment–level data
- Integration with map and routing data

---

## System Design

The project follows an industry-style ML system pipeline:

### 1. Data Ingestion
- Raw datasets are stored immutably in `data/raw/`
- No manual modification of raw files

### 2. Data Cleaning & Processing
- Timestamp parsing and validation
- Sorting by location and time
- Removal of invalid or missing observations
- Cleaned datasets saved to `data/processed/`

### 3. Feature Engineering
- Rolling window features capturing recent traffic behavior
- Temporal features (hour of day, day of week, weekend indicators)
- Structured to support future contextual features (e.g., weather)

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
│   ├── raw/            # original datasets
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
