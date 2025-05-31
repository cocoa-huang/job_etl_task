import os
import json
import redis


class RedisConnector:
    """
    Connector class for Redis operations (caching and deduplication)
    """
    def __init__(self, url=None):
        self.url = url or os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        self.client = None
        self.connect()

    def connect(self):
        """
        Establish connection to Redis
        """
        try:
            self.client = redis.from_url(self.url)
            # Test connection
            self.client.ping()
            print(f"Successfully connected to Redis at {self.url}")
        except redis.ConnectionError as e:
            print(f"Failed to connect to Redis: {e}")
            raise

    def close(self):
        """
        Close the Redis connection
        """
        if self.client:
            self.client.close()
            print("Redis connection closed")

    def set_item(self, key, value, expiry=None):
        """
        Set a key-value pair in Redis
        """
        if isinstance(value, dict):
            value = json.dumps(value)
        if expiry:
            return self.client.setex(key, expiry, value)
        return self.client.set(key, value)

    def get_item(self, key, parse_json=True):
        """
        Get a value from Redis by key
        """
        value = self.client.get(key)
        if value and parse_json:
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
        return value

    def delete_item(self, key):
        """
        Delete a key from Redis
        """
        return self.client.delete(key)

    def exists(self, key):
        """
        Check if a key exists in Redis
        """
        return self.client.exists(key)

    def set_in_set(self, set_name, value):
        """
        Add a value to a Redis set (useful for deduplication)
        """
        return self.client.sadd(set_name, value)

    def item_in_set(self, set_name, value):
        """
        Check if a value exists in a Redis set
        """
        return self.client.sismember(set_name, value)

    def cache_item(self, item, collection_name):
        """
        Cache an item with a unique key based on its ID or fingerprint
        """
        # Generate a unique key based on your data
        item_id = item.get('id') or item.get('_id')
        if not item_id:
            return False
        
        key = f"{collection_name}:{item_id}"
        return self.set_item(key, item)

    def is_duplicate(self, item, set_name='seen_items'):
        """
        Check if an item is a duplicate by using a fingerprint
        """
        # Generate a fingerprint for the item
        fingerprint = self._generate_fingerprint(item)
        
        if self.item_in_set(set_name, fingerprint):
            return True
        
        # Add to set to track for future duplicate checks
        self.set_in_set(set_name, fingerprint)
        return False
    
    def _generate_fingerprint(self, item):
        """
        Generate a unique fingerprint for an item to detect duplicates
        """
        if isinstance(item, dict):
            # Use an identifier field if available
            if 'id' in item:
                return str(item['id'])
            elif 'url' in item:
                return str(item['url'])
            
            # Otherwise, create a fingerprint from common fields
            fingerprint_parts = []
            for key in sorted(item.keys()):
                if key in ['title', 'name', 'url', 'id', '_id']:
                    fingerprint_parts.append(f"{key}:{item[key]}")
            
            if fingerprint_parts:
                return "|".join(fingerprint_parts)
            
            # Fallback to a JSON string of the entire item
            return json.dumps(item, sort_keys=True)
        
        # If item is not a dict, use its string representation
        return str(item)
