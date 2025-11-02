import os
from pyspark.sql import SparkSession
from sparkmeasure import StageMetrics
import time
import json

os.environ["SPARK_MASTER_HOST"] = "local[*]"
os.environ["DATASET_PATH"] = "./data/train.csv"
os.environ["DRIVER_MEMORY"] = "4g"

from data_science.main import run_data_cleaning
from data_science.connectors import getNewSparkSession

def run_local_benchmark(dataset_scale=0.1):
    """Run benchmark locally without Docker."""
    
    # http_proxy = os.getenv("http_proxy", "")
    # https_proxy = os.getenv("https_proxy", "")
    
    # builder = SparkSession.builder \
    #     .appName("LocalBenchmark") \
    #     .master("local[2]") \
    #     .config("spark.driver.memory", "4g") \
    #     .config("spark.executor.memory", "2g") \
    #     .config("spark.jars.packages", "ch.cern.sparkmeasure:spark-measure_2.13:0.27")
    
    # if http_proxy or https_proxy:
    #     java_opts = []
    #     if http_proxy:
    #         from urllib.parse import urlparse
    #         parsed = urlparse(http_proxy)
    #         if parsed.hostname and parsed.port:
    #             java_opts.append(f"-Dhttp.proxyHost={parsed.hostname}")
    #             java_opts.append(f"-Dhttp.proxyPort={parsed.port}")
    #     if https_proxy:
    #         from urllib.parse import urlparse
    #         parsed = urlparse(https_proxy)
    #         if parsed.hostname and parsed.port:
    #             java_opts.append(f"-Dhttps.proxyHost={parsed.hostname}")
    #             java_opts.append(f"-Dhttps.proxyPort={parsed.port}")
        
    #     if java_opts:
    #         builder = builder.config("spark.driver.extraJavaOptions", " ".join(java_opts))
    
    # spark = builder.getOrCreate()
    
    spark = getNewSparkSession(num_workers=1, mem_per_worker=2, cores_per_worker=2)
    
    print(spark.sparkContext.getConf().getAll())
    print("Spark session created successfully")

    dataset_path = "./data/train.csv"
    full_df = spark.read.csv(dataset_path, header=True, inferSchema=True)
    print(full_df.head())
    
    total_rows = full_df.count()
    num_rows = int(total_rows * dataset_scale)
    df = full_df.limit(num_rows)
    
    print(f"Processing {num_rows} rows ({dataset_scale*100}% of dataset)")
    
    stagemetrics = StageMetrics(spark)
    
    start_time = time.time()
    stagemetrics.begin()
    
    run_data_cleaning(df=df)
    
    stagemetrics.end()
    elapsed_time = time.time() - start_time
    
    print(f"\nBenchmark completed!")
    print(f"Time: {elapsed_time:.2f} seconds")
    print(f"Throughput: {num_rows / elapsed_time:.2f} rows/sec")
    
    stagemetrics.print_report()
    
    print(f"\n{'='*60}")
    print(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")
    print(f"Press Ctrl+C to stop and exit")
    print(f"{'='*60}\n")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping Spark session...")
        spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    run_local_benchmark(dataset_scale=1)
