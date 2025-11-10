# 🎵 Spotify Intelligence

A data engineering project that leverages Spotify's API to collect, process, and analyze music data using modern data engineering practices.

## 🎯 Project Overview

This project implements a medallion architecture (Bronze → Silver → Gold) to process Spotify data:
- **Bronze Layer**: Raw data ingestion from Spotify API
- **Silver Layer**: Cleaned and transformed data
- **Gold Layer**: Analytics-ready datasets

## 🛠️ Tech Stack

- **Python 3.14**
- **Polars**: High-performance DataFrame library
- **Delta Lake**: Reliable data storage format
- **Spotipy**: Spotify Web API wrapper
- **WSL**: Windows Subsystem for Linux development environment

## 🏗️ Project Structure

```
spotify_intelligence/
├── src/
│   ├── spotify_intelligence/
│   │   ├── bronze_layer/    # Raw data ingestion
│   │   └── silver_layer/    # Data transformation
│   │   └── gold_layer/      # Data Refinements for analytics
│   └── Utils/               # Shared utilities
└── test/                    # Test suite
```

## 🚀 Getting Started

1. **Clone the repository**
```bash
git clone https://github.com/MehdiSensali/spotify_intelligence.git
cd spotify_intelligence
```

2. **Set up Python virtual environment**
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/WSL
```

3. **Install your project locally (this will also install the requirements)**
```bash
pip install -e .
```

4. **Configure Spotify API credentials**
```bash
# Add to your shell environment or .bashrc
export SPOTIFY_CLIENT_ID="your_client_id"
export SPOTIFY_CLIENT_SECRET="your_client_secret"
```

## 📊 Features

- Fetch artist, album, and track data from Spotify
- Store data in Delta format for reliability
- Transform raw data into analytics-ready tables
- Explore data using Jupyter notebooks
- Type-hinted Python code for better development experience

## 🔄 Data Pipeline

1. **Bronze Layer**
   - Raw data ingestion from Spotify API
   - Preserve original data structure
   - Delta Lake storage for reliability

2. **Silver Layer**
   - Data cleaning and transformation
   - Schema standardization
   - Quality checks

3. **Gold Layer**
   - Analytics-ready datasets
   - Aggregated views
   - Performance optimizations

## 🧪 Testing

```bash
# Run tests
pytest test/
```

## 📝 License

[MIT License](LICENSE)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📫 Contact

Mehdi Sensali - [GitHub Profile](https://github.com/MehdiSensali)
