BOT_NAME = "jobs_project"

SPIDER_MODULES = ["jobs_project.spiders"]
NEWSPIDER_MODULE = "jobs_project.spiders"

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy
CONCURRENT_REQUESTS = 8

# Configure a delay for requests for the same website
DOWNLOAD_DELAY = 1

# Configure item pipelines
ITEM_PIPELINES = {
    "jobs_project.pipelines.JobsProjectPipeline": 300,
    "jobs_project.pipelines.MongoDBPipeline": 400,
    "jobs_project.pipelines.RedisDuplicationPipeline": 200,
}

# Enable and configure the AutoThrottle extension
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 5
AUTOTHROTTLE_MAX_DELAY = 60
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
AUTOTHROTTLE_DEBUG = False

# MongoDB Settings
MONGO_URI = "mongodb://mongodb:27017"
MONGO_DATABASE = "nlp"
MONGO_COLLECTION = "nlp"

# Redis Settings
REDIS_URL = "redis://redis:6379/0"
REDIS_DUPLICATE_SET = "seen_items"

# Configure logging level
LOG_LEVEL = "INFO"

# File handling settings
FILES_STORE = 'data'
