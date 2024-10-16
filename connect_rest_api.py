from pyspark.sql import SparkSession
import requests
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Load Data from REST API") \
    .getOrCreate()

# URL of the REST API
url = "https://jsonplaceholder.org/posts/"

# Fetch data from the REST API
response = requests.get(url)

# Fetch data from the REST API with basic authentication
#response = requests.get(url, auth=(username, password))

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON data
    data = response.json()
    
    # Convert JSON data to DataFrame
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(data)]))
    
    # Show the DataFrame
    df.show()
else:
    print(f"Failed to fetch data: {response.status_code}")

# Stop the Spark session
spark.stop()
