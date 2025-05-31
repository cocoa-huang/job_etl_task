# Job Scraper Project

A comprehensive job scraping system built with Scrapy, MongoDB, and Redis.

## Overview

This project provides an ETL pipeline for job data:
- Extract: Scrapes job data from JSON files using Scrapy
- Transform: Processes and validates the data
- Load: Stores structured data in MongoDB and exports to CSV
- Deduplication: Uses Redis to prevent duplicate entries across runs

## Setup Instructions

### Prerequisites
- Docker and Docker Compose
- Python 3.9+ (if running locally)

### Installation

1. Start services with Docker:
   ```
   docker-compose up -d
   ```

## Usage

### Complete Workflow

1. **Start the services**
   ```
   docker-compose up -d
   ```

2. **Reset the database and cache (optional, but recommended for clean runs)**
   ```
   # Clear Redis cache 
   docker exec -it redis redis-cli FLUSHALL
   
   # Clear MongoDB collection
   docker exec -it mongodb mongosh --eval "use jobs" --eval "db.jobs.drop()"
   ```

3. **Run the spider to collect data**
   ```
   docker exec -it scrapy_service bash -c "cd jobs_project && scrapy crawl json_jobs"
   ```

4. **Export data to CSV**
   ```
   docker exec -it scrapy_service bash -c "python query.py --no-spider"
   ```


### Important Notes

- **Data Files**: The source JSON files are located in `jobs_project/data/` and contain the raw job data to be processed.

### Project Structure

```
.
├── dockerfile              # Docker container configuration
├── docker-compose.yaml     # Multi-container Docker setup
├── requirements.txt        # Python dependencies
├── query.py                # Main entry point for running scrapers
├── jobs_project/           # Scrapy project folder
│   ├── data/               # Data output directory
│   ├── jobs_project/       # Scrapy app
│   │   ├── spiders/        # Scrapy spiders
│   │   ├── items.py        # Data structure definitions
│   │   ├── pipelines.py    # Data processing pipelines
│   │   └── settings.py     # Scrapy settings
│   └── scrapy.cfg          # Scrapy configuration
└── infra/                  # Infrastructure components
    ├── mongodb_connector.py # MongoDB connection handler
    └── redis_connector.py   # Redis connection handler
```

## Configuration

The project uses environment variables for configuration. Key variables include:

- `MONGO_URI`: MongoDB connection string (default: `mongodb://mongodb:27017/`)
- `MONGO_DATABASE`: MongoDB database name (default: `jobs`)
- `MONGO_COLLECTION`: MongoDB collection name (default: `jobs`)
- `REDIS_URL`: Redis connection string (default: `redis://redis:6379/0`)

These can be set in the `docker-compose.yaml` file or as environment variables.

