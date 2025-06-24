import satbucket

# Define bucket directory
bucket_dir = "/home/ghiggi/data/AVHRR_Bucket"

# Read time series around a given point of interest
df = satbucket.read(
    bucket_dir=bucket_dir,
    use_pyarrow=False,  # use rust parquet reader
    point=(-83, 62), # (lon, lat)
    distance=5000,  # meters
    parallel="auto", # "row_groups", "columns"
    backend="pandas" # "pandas"  
)

print(df)

