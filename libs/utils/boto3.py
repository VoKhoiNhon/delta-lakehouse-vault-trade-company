import os
import sys


# s3_resource = boto3.resource(
#     's3',
#     aws_access_key_id=ACCESS_KEY,
#     aws_secret_access_key=SECRET_KEY,
#     endpoint_url=ENDPOINT_URL
# )

# bucket = 'lakehouse-raw'
# prefix = '1tm_2412/delta_Arizona'
# upload_folder_to_s3(s3_resource, prefix, bucket, prefix)
# s3_download_directory(s3_resource, bucket, prefix)


def print_progress(current, total, action="Progress"):
    percentage = (current / total) * 100
    sys.stdout.write(f"\r{action}: {percentage:.1f}% ({current}/{total} bytes)")
    sys.stdout.flush()


def s3_download_directory(s3_resource, bucket, prefix):
    bucket = s3_resource.Bucket(bucket)
    _prefixs = prefix.split("/")

    # Create parent directories
    for i, p in enumerate(_prefixs):
        _path = "/".join(_prefixs[: i + 1])
        if not os.path.exists(os.path.dirname(_path)):
            try:
                os.makedirs(_path)
            except:
                pass

    # Get list of all objects to download
    objects = list(bucket.objects.filter(Prefix=prefix))
    total_size = sum(obj.size for obj in objects)

    print(f"Total size to download: {total_size/1024/1024:.2f} MB")

    for obj in objects:
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))

        if not os.path.isfile(obj.key):
            bucket.download_file(
                obj.key,
                obj.key,
            )

    print("\nDownload completed!")


def upload_folder_to_s3(s3_resource, inputDir, bucket, prefix, suffix=None):
    upload_files = []

    for path, subdirs, files in os.walk(inputDir):
        if len(files) > 0:
            for file in files:
                _file = os.path.join(path, file)
                if suffix is None or _file.endswith(suffix):
                    upload_files.append(_file)

    total_size = sum(os.path.getsize(f) for f in upload_files)

    print(f"Total size to upload: {total_size/1024/1024:.2f} MB")

    for _file in upload_files:
        key = prefix + _file.replace(inputDir, "")
        print(f"\nUploading: {_file} -> {key}")

        s3_resource.Bucket(bucket).upload_file(
            _file,
            key,
            # Callback=progress_callback
        )

    # Ensure 100% progress is shown
    print_progress(total_size, total_size, "Uploading")
    print("\nUpload completed!")


def delete_s3_folder(s3_client, bucket: str, folder: str) -> bool:
    """
    Delete folder và contents trong S3 bucket
    """
    folder = folder.rstrip("/") + "/"

    try:
        # List và xóa objects
        paginator = s3_client.get_paginator("list_objects_v2")
        delete_list = []

        for page in paginator.paginate(Bucket=bucket, Prefix=folder):
            if "Contents" in page:
                delete_list.extend([{"Key": obj["Key"]} for obj in page["Contents"]])

        if delete_list:
            # Xóa objects theo batch 1000
            for i in range(0, len(delete_list), 1000):
                s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": delete_list[i : i + 1000], "Quiet": True},
                )
        print(f"Deleted bucket:{bucket} folder:{folder}")
        return True

    except Exception as e:
        print(f"Error deleting folder: {str(e)}")
        return False


def delete_s3_folder_with_spark_sesion(spark, s3_path) -> bool:
    """
    Args:
        spark (_type_): _description_
        s3_path (_type_): "s3a://lakehouse-bronze/1tm_2412/company_tmp_pure_name"

    Returns:
        bool:
    """
    endpoint_url = "https://" + spark.sparkContext.getConf().get(
        "spark.hadoop.fs.s3a.endpoint"
    )
    aws_access_key_id = spark.sparkContext.getConf().get(
        "spark.hadoop.fs.s3a.access.key"
    )
    aws_secret_access_key = spark.sparkContext.getConf().get(
        "spark.hadoop.fs.s3a.secret.key"
    )
    import boto3

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
    )

    parts = s3_path.replace("s3a://", "").split("/", 1)
    bucket = parts[0]
    key = parts[1]
    return delete_s3_folder(s3_client, bucket, key)
