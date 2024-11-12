"""
Parameters used for the processing.
"""

import os

from consts import constants as const
from log.logger import logger

aws_access_key_id = os.getenv(const.AWS_ACCESS_KEY_ID)
aws_secret_access_key = os.getenv(const.AWS_SECRET_ACCESS_KEY)
bucket = os.getenv(const.S3_BUCKET, "data-engineer-assignment-clintfeher")
if not all([aws_access_key_id, aws_secret_access_key, bucket]):
    logger.error(
        "Error initializing application, make sure the keys [%s, %s, %s] exist in the environment variables",
        const.AWS_ACCESS_KEY_ID,
        const.AWS_SECRET_ACCESS_KEY,
        const.S3_BUCKET,
    )
    raise Exception(
        "Halting application, please add the required keys as in environment variables."
    )
base_folder = os.path.join("s3a://", bucket, "before_deployment")
incoming_table_folder = os.path.join(base_folder, "incoming")
