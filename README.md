# Market Data Pipeline

This repository contains a collection of Python scripts and tools for collecting, processing, and storing financial market data, including stock and cryptocurrency data. The data is fetched from various APIs such as Yahoo Finance and CoinGecko, and stored in SQLite databases for further analysis.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
  - [Collecting Data](#collecting-data)
  - [Cleaning Data](#cleaning-data)
  - [Automating with Cron Jobs](#automating-with-cron-jobs)
- [File Descriptions](#file-descriptions)
- [Contributing](#contributing)
- [License](#license)

## Overview

This project provides a pipeline for gathering, processing, and managing market data. It includes scripts to:

- Fetch daily stock and cryptocurrency data.
- Fetch intraday 1-minute interval data for stocks and cryptocurrencies.
- Store the data in SQLite databases.
- Clean duplicate records from the databases.
- Schedule automated data collection using cron jobs.

## Features

- **Data Collection**: Fetch historical and real-time data for stocks and cryptocurrencies.
- **Data Storage**: Store data in local SQLite databases.
- **Data Cleaning**: Remove duplicate entries from the databases.
- **Automation**: Set up cron jobs for automated data collection.

## Getting Started

### Prerequisites

Before you begin, ensure you have the following installed:

- Python 3.6+
- Pip (Python package installer)
- SQLite (usually included with Python)
- Git

### Installation

1. **Clone the repository:**

   ```bash
   git clone git@github.com:anandpatel010/market-data-pipeline.git
   cd market-data-pipeline
