import json
import os
import scrapy
from scrapy.exceptions import DropItem
from jobs_project.items import JobItem


class JsonJobSpider(scrapy.Spider):
    name = 'json_jobs'
    
    def __init__(self, *args, **kwargs):
        super(JsonJobSpider, self).__init__(*args, **kwargs)
        # Define the data directory
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data')
        self.files = ['s01.json', 's02.json']
    
    def start_requests(self):
        """Generate requests for each JSON file"""
        for filename in self.files:
            file_path = os.path.join(self.data_dir, filename)
            if os.path.exists(file_path):
                # Create a Request object with the file path as the URL
                # We'll use the file:// scheme to process local files
                url = f"file://{file_path}"
                yield scrapy.Request(url=url, callback=self.parse_json, meta={'filename': filename})
            else:
                self.logger.error(f"File {file_path} does not exist!")
    
    def parse_json(self, response):
        """Parse the JSON file contents"""
        try:
            # Load JSON data
            data = json.loads(response.text)
            
            # Extract jobs from the 'jobs' key
            jobs = data.get('jobs', [])
            
            self.logger.info(f"Processing {len(jobs)} jobs from {response.meta['filename']}")
            
            # Process each job entry
            for job in jobs:
                # Extract job data
                job_data = job.get('data', {})
                
                # Create a JobItem with the extracted data
                job_item = JobItem()
                
                # Map job data to item fields
                job_item['id'] = job_data.get('slug') or job_data.get('req_id')
                job_item['title'] = job_data.get('title')
                job_item['company'] = job_data.get('hiring_organization') or 'FedEx'
                job_item['location'] = job_data.get('full_location')
                job_item['description'] = job_data.get('description')
                job_item['salary'] = job_data.get('salary_value')
                job_item['url'] = job_data.get('apply_url')
                job_item['posted_date'] = job_data.get('posted_date') or job_data.get('create_date')
                job_item['job_type'] = job_data.get('employment_type')
                job_item['industry'] = job_data.get('category', [])
                job_item['benefits'] = job_data.get('benefits', [])
                job_item['source'] = job_data.get('source') or response.meta['filename']
                job_item['created_at'] = None  # Will be set by pipeline
                job_item['updated_at'] = None  # Will be set by pipeline
                
                # Additional location details if available
                if job_data.get('street_address'):
                    location_parts = []
                    if job_data.get('street_address'):
                        location_parts.append(job_data.get('street_address'))
                    if job_data.get('city'):
                        location_parts.append(job_data.get('city'))
                    if job_data.get('state'):
                        location_parts.append(job_data.get('state'))
                    if job_data.get('country'):
                        location_parts.append(job_data.get('country'))
                    if job_data.get('postal_code'):
                        location_parts.append(job_data.get('postal_code'))
                    
                    job_item['location'] = ", ".join(filter(None, location_parts))
                
                # Generate a fingerprint for deduplication
                # This will be used by the Redis deduplication pipeline
                job_item['_fingerprint'] = f"{job_item['id']}:{job_item['title']}:{job_item['company']}"
                
                yield job_item
                
        except json.JSONDecodeError:
            self.logger.error(f"Failed to parse JSON from {response.meta['filename']}")
        except Exception as e:
            self.logger.error(f"Error processing {response.meta['filename']}: {e}")
