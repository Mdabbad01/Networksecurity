import os
from networksecurity.logging.logger import logger

class S3Sync:
    def sync_folder_to_s3(self, folder: str, aws_bucket_url: str):
        # Ensure folder exists (empty folders won't sync)
        if not os.path.exists(folder):
            logger.info(f"Folder {folder} does not exist. Creating placeholder.")
            os.makedirs(folder, exist_ok=True)
            placeholder = os.path.join(folder, ".keep")
            with open(placeholder, "w") as f:
                f.write("keep")
        command = f"aws s3 sync {folder} {aws_bucket_url}"
        logger.info(f"Running command: {command}")
        exit_code = os.system(command)
        if exit_code != 0:
            logger.error(f"Failed to sync {folder} to {aws_bucket_url}")

    def sync_folder_from_s3(self, folder: str, aws_bucket_url: str):
        command = f"aws s3 sync {aws_bucket_url} {folder}"
        logger.info(f"Running command: {command}")
        exit_code = os.system(command)
        if exit_code != 0:
            logger.error(f"Failed to sync {aws_bucket_url} to {folder}")
