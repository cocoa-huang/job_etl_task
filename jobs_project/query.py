#!/usr/bin/env python3
import csv
import os
import sys
from datetime import datetime

# Add the parent directory to sys.path to import from infra
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from infra.mongodb_connector import MongoDBConnector


def export_to_csv(data, filename):
    """
    Export data to a CSV file
    
    Args:
        data (list): List of dictionaries containing the data to export
        filename (str): Name of the CSV file to create
    """
    if not data:
        print("No data to export")
        return
    
    # Get fieldnames from the first document
    fieldnames = list(data[0].keys())
    
    # Use the existing data directory in jobs_project
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    os.makedirs(data_dir, exist_ok=True)
    filepath = os.path.join(data_dir, filename)
    
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Data successfully exported to {filepath}")
    print(f"Total records exported: {len(data)}")


def main():
    """
    Main function to retrieve data from MongoDB and export to CSV
    """
    # Connect to MongoDB
    mongo_connector = MongoDBConnector()
    
    try:
        # Get the collection name from settings or use default
        collection_name = 'jobs'
        
        # Count total documents
        total_docs = mongo_connector.count(collection_name)
        print(f"Total documents in {collection_name}: {total_docs}")
        
        # Retrieve all processed jobs
        jobs = mongo_connector.find_many(collection_name)
        
        if jobs:
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"final_jobs_{timestamp}.csv"
            
            # Export to CSV
            export_to_csv(jobs, filename)
        else:
            print("No job data found in the database")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close MongoDB connection
        mongo_connector.close()


if __name__ == "__main__":
    main() 