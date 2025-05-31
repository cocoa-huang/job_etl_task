import os
import sys
import csv
import logging
from datetime import datetime

# Set up paths for proper module discovery
jobs_project_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'jobs_project')
sys.path.insert(0, jobs_project_path)

# Set Scrapy settings module environment variable - using the correct path
os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'jobs_project.settings')

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from infra.mongodb_connector import MongoDBConnector
from infra.redis_connector import RedisConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_spider():
    """Run the JSON spider to scrape job data from JSON files"""
    logger.info("Starting the job scraping process...")
    
    # Get project settings
    settings = get_project_settings()
    
    # Create a crawler process
    process = CrawlerProcess(settings)
    
    # Add the JSON spider to the process
    process.crawl('json_jobs')
    
    # Start the process
    process.start()
    
    logger.info("Scraping process completed.")

def query_mongodb():
    """Query MongoDB to check if data was successfully stored"""
    logger.info("Querying MongoDB for stored job data...")
    
    try:
        # Connect to MongoDB
        mongodb = MongoDBConnector()
        
        # Get the collection name from settings or use default
        collection_name = os.getenv('MONGO_COLLECTION', 'jobs')
        
        # Count all jobs
        total_jobs = mongodb.count(collection_name)
        logger.info(f"Total jobs in MongoDB: {total_jobs}")
        
        # Get a sample job
        sample_job = mongodb.find_one(collection_name)
        if sample_job:
            logger.info(f"Sample job: {sample_job.get('title')} at {sample_job.get('company')}")
        
        # Export all jobs to CSV
        export_to_csv(mongodb, collection_name)
        
        # Close the connection
        mongodb.close()
        
    except Exception as e:
        logger.error(f"Error querying MongoDB: {e}")

def export_to_csv(mongodb, collection_name):
    """Export MongoDB data to CSV file"""
    logger.info("Exporting job data to CSV...")
    
    try:
        # Get all jobs
        jobs = mongodb.find_many(collection_name)
        
        if not jobs:
            logger.warning("No jobs found to export.")
            return
        
        # Define output file with timestamp
        data_dir = os.path.join('jobs_project', 'data')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(data_dir, f'final_jobs_{timestamp}.csv')
        
        # Define CSV fields (based on job item structure)
        fields = [
            'id', 'title', 'company', 'location', 'description', 
            'salary', 'url', 'posted_date', 'job_type', 'industry',
            'benefits', 'source', 'created_at', 'updated_at'
        ]
        
        # Write to CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fields, extrasaction='ignore')
            writer.writeheader()
            
            # Process each job
            for job in jobs:
                # Handle special field types
                if 'industry' in job and isinstance(job['industry'], list):
                    job['industry'] = ', '.join(job['industry'])
                if 'benefits' in job and isinstance(job['benefits'], list):
                    job['benefits'] = ', '.join(job['benefits'])
                
                writer.writerow(job)
        
        logger.info(f"Successfully exported {len(jobs)} jobs to {output_file}")
        
    except Exception as e:
        logger.error(f"Error exporting to CSV: {e}")

if __name__ == "__main__":
    # Check if we should run the spider or just query MongoDB
    if len(sys.argv) > 1 and sys.argv[1] == '--no-spider':
        logger.info("Skipping spider execution, only querying MongoDB and exporting data...")
    else:
        # Run the spider
        run_spider()
    
    # Query MongoDB to verify data was stored and export to CSV
    query_mongodb()