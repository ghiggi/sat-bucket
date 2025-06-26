####----------------------------------------------------------------------.
#### REQUIREMENT
# Before running this script, change the ulimit (Open File Limit) close to hard limit 
# --> https://stackoverflow.com/questions/34588/how-do-i-change-the-number-of-open-files-limit-in-linux
# --> See hard limit with: ulimit -Hn
# --> See current soft limit with: limit -Sn
# --> Modify soft limit with: ulimit -n 999999
from satbucket import merge_granule_buckets


if __name__ == "__main__":
    ####----------------------------------------------------------------------.
    #### Define bucket filepaths 
    src_bucket_dir = "/home/ghiggi/data/GPM_DPR_Bucket_Granules/"
    dst_bucket_dir = "/home/ghiggi/data/GPM_DPR_Bucket"
    
    # 2A.GPM.DPR.V9-20211125.20190717-S211332-E224606.030588.V07A.HDF5
    filename_pattern = "{product_level:s}.{satellite:s}.{sensor:s}.{algorithm:s}.{start_time:%Y%m%d-S%H%M%S}-E{end_time:%H%M%S}.{granule_id}.{version}.{data_format}"
    
    ####----------------------------------------------------------------------.
    #### Merge Parquet Datasets     
    merge_granule_buckets(
        # Bucket directories
        src_bucket_dir=src_bucket_dir,
        dst_bucket_dir=dst_bucket_dir,
        filename_pattern=filename_pattern,
        # Consolidation options
        row_group_size="200MB",
        max_file_size="2GB",
        max_open_files=0,
        use_threads=True,
        compression="snappy",
        compression_level=None, 
        # Scanner options
        batch_size=131_072,
        batch_readahead=10,  # 16
        fragment_readahead=20,  # 4
    )
    ####----------------------------------------------------------------------.


 
