import datetime
import os
import sys
from datetime import timezone

# Add the parent directory to sys.path to be able to import from infra
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from infra.mongodb_connector import MongoDBConnector
from infra.redis_connector import RedisConnector
from scrapy.exceptions import DropItem


class JobsProjectPipeline:
    """Basic pipeline for processing job items"""
    
    def process_item(self, item, spider):
        """Process each item"""
        # Set timestamps
        now = datetime.datetime.now(datetime.timezone.utc)
        if 'created_at' not in item or item['created_at'] is None:
            item['created_at'] = now
        item['updated_at'] = now
        
        # Clean up fields
        for key, value in dict(item).items():
            if value is None or value == "":
                del item[key]
        
        # Add source if not present
        if 'source' not in item:
            item['source'] = spider.name
        
        return item


class MongoDBPipeline:
    """Pipeline for storing items in MongoDB"""
    
    def __init__(self, mongo_uri, mongo_db, mongo_collection):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.mongo_connector = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE'),
            mongo_collection=crawler.settings.get('MONGO_COLLECTION')
        )

    def open_spider(self, spider):
        """When spider opens, connect to MongoDB"""
        self.mongo_connector = MongoDBConnector(
            uri=self.mongo_uri,
            db_name=self.mongo_db
        )
        spider.logger.info(f"MongoDB pipeline opened with database: {self.mongo_db}")

    def close_spider(self, spider):
        """When spider closes, close MongoDB connection"""
        if self.mongo_connector:
            self.mongo_connector.close()
        spider.logger.info("MongoDB pipeline closed")

    def process_item(self, item, spider):
        """Store item in MongoDB"""
        # Convert Scrapy item to dict
        item_dict = dict(item)
        
        # Store in MongoDB
        try:
            inserted_id = self.mongo_connector.insert_one(
                self.mongo_collection, 
                item_dict
            )
            spider.logger.debug(f"Item saved to MongoDB with ID: {inserted_id}")
        except Exception as e:
            spider.logger.error(f"Failed to save item to MongoDB: {e}")
        
        return item


class RedisDuplicationPipeline:
    """Pipeline for checking duplicates using Redis"""
    
    def __init__(self, redis_url, redis_set):
        self.redis_url = redis_url
        self.redis_set = redis_set
        self.redis_connector = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            redis_url=crawler.settings.get('REDIS_URL'),
            redis_set=crawler.settings.get('REDIS_DUPLICATE_SET')
        )

    def open_spider(self, spider):
        """When spider opens, connect to Redis"""
        self.redis_connector = RedisConnector(url=self.redis_url)
        spider.logger.info(f"Redis pipeline opened using set: {self.redis_set}")

    def close_spider(self, spider):
        """When spider closes, close Redis connection"""
        if self.redis_connector:
            self.redis_connector.close()
        spider.logger.info("Redis pipeline closed")

    def process_item(self, item, spider):
        """Check if item is a duplicate and drop if it is"""
        # Check if item is a duplicate
        if self.redis_connector.is_duplicate(dict(item), self.redis_set):
            spider.logger.info(f"Duplicate item found and dropped: {item.get('id', 'Unknown ID')}")
            # Drop item by raising DropItem
            raise DropItem(f"Duplicate item found: {item.get('id', 'Unknown ID')}")
        
        # Set the item in cache for faster access
        if 'id' in item:
            self.redis_connector.cache_item(dict(item), 'job_cache')
        
        return item
