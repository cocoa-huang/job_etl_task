import scrapy
from datetime import datetime, timezone

class JobItem(scrapy.Item):
    # Primary fields
    id = scrapy.Field()
    title = scrapy.Field()
    company = scrapy.Field()
    location = scrapy.Field()
    description = scrapy.Field()
    salary = scrapy.Field()
    url = scrapy.Field()
    posted_date = scrapy.Field()
    job_type = scrapy.Field()
    industry = scrapy.Field()
    benefits = scrapy.Field()
    source = scrapy.Field()
    
    # Meta fields for processing
    created_at = scrapy.Field(serializer=lambda x: datetime.now(timezone.UTC) if x is None else x)
    updated_at = scrapy.Field(serializer=lambda x: datetime.now(timezone.UTC))
    _fingerprint = scrapy.Field()  # For deduplication
