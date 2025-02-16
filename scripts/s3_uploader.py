import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def upload_folder_to_s3(local_folder, bucket_name, s3_folder, s3_client):
    """
    Upload all files in the local folder to S3.

    :param local_folder: Local Folder Path
    :param bucket_name: S3 Bucket Name
    :param s3_folder: S3 folder path
    """
    # Initialize S3 Client
    s3 = s3_client

    # Iterate through all files in the local folder
    for root, dirs, files in os.walk(local_folder):
        for file in files:
            # Local File Path
            local_file_path = os.path.join(root, file)

            # S3 File paths (preserving folder structure)
            relative_path = os.path.relpath(local_file_path, local_folder)
            s3_file_path = os.path.join(s3_folder, relative_path).replace("\\", "/")

            try:
                # upload to S3
                s3.upload_file(local_file_path, bucket_name, s3_file_path)
                print(f"File uploaded successfully: {local_file_path} -> {s3_file_path}")
            except (NoCredentialsError, PartialCredentialsError):
                print("AWS credentials are not configured or are invalid.")
            except Exception as e:
                print(f"File upload failed: {local_file_path} -> {s3_file_path}, 错误: {e}")

def upload_folder_to_s3_multipart(local_folder, bucket_name, s3_folder):
    """
    Use the Segmented Upload feature to upload all files in a local folder to S3.

    :param local_folder: Local Folder Path
    :param bucket_name: S3 Bucket Name
    :param s3_folder: S3 folder path
    """
    s3 = boto3.client("s3")
    config = boto3.s3.transfer.TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10)  # 配置分段上传

    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_folder)
            s3_file_path = os.path.join(s3_folder, relative_path).replace("\\", "/")

            try:
                s3.upload_file(local_file_path, bucket_name, s3_file_path, Config=config)
                print(f"File uploaded successfully: {local_file_path} -> {s3_file_path}")
            except Exception as e:
                print(f"File upload failed: {local_file_path} -> {s3_file_path}, error: {e}")