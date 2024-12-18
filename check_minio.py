#!/usr/bin/env python3

import subprocess
import json
import sys
from typing import Any, Tuple

STATUS_STRINGS = ['OK', 'WARNING', 'CRITICAL', 'UNKNOWN']


def get_buckets():
    """Retrieve the list of buckets from MinIO."""
    try:
        out = subprocess.check_output(['/opt/minio-binaries/mc', 'ls', '--insecure', '--json', 'minio']).decode('utf-8').splitlines()
        buckets = [json.loads(line) for line in out]
        return [str(item.get("key")) for item in buckets if "key" in item]
    except Exception as e:
        print(f"Error retrieving buckets: {e}")
        return []


def get_bucket_size(bucket: str):
    """Retrieve the bucket's quota from MinIO."""
    try:
        out = subprocess.check_output(['/opt/minio-binaries/mc', 'du', '--insecure', '--json', f'minio/{bucket}']).decode('utf-8').strip()
        bucket_info = json.loads(out)
        return int(bucket_info.get("size")), int(bucket_info.get("objects"))
    except Exception as e:
        print(f"Error retrieving bucket's size: {e}")
        return None, None


def get_bucket_quota(bucket_name: str):
    """Retrieve the bucket's size from MinIO."""
    try:
        out = subprocess.check_output(['/opt/minio-binaries/mc', 'quota', 'info', '--insecure', '--json', f'minio/{bucket_name}']).decode('utf-8').strip()
        bucket_info = json.loads(out)
        if bucket_info.get("quota"):
            return int(bucket_info.get("quota"))
        else:
            return None
    except Exception as e:
        print(f"Error retrieving bucket's quota: {e}")
        return None


def check_bucket(bucket_name: str):
    """Check a bucket's size and quota, and determine its status."""
    # Retrieve bucket size and quota
    quota_bytes = get_bucket_quota(bucket_name)  # Returns quota in bytes
    size_bytes, objects = get_bucket_size(bucket_name)  # Returns size in bytes

    # Validate retrieval
    if quota_bytes is None or size_bytes is None:
        # print(f"Error: Could not retrieve quota or size for bucket '{bucket_name}'.")
        if not quota_bytes and size_bytes:
            return 1, size_bytes / (1024 ** 3), None, None, objects # WARNING status if quota is not set
        else:
            return 3, None, None, None, None  # UNKNOWN status if retrieval fails

    # Convert size and quota to GB
    size_gb = size_bytes / (1024 ** 3)  # Convert bytes to GB
    quota_gb = quota_bytes / (1024 ** 3)

    # Calculate usage percentage
    usage_percentage = (size_gb / quota_gb) * 100 if quota_gb > 0 else 0

    # Determine status
    if usage_percentage >= 90:
        status = 2  # CRITICAL
    elif usage_percentage > 80:
        status = 1  # WARNING
    else:
        status = 0  # OK

    return status, size_gb, quota_gb, usage_percentage, objects


def get_minio_info():
    """Retrieve MinIO server information in JSON format."""
    try:
        out = subprocess.check_output(['/opt/minio-binaries/mc', 'admin', 'info', '--insecure', '--json', 'minio'])
        return json.loads(out)
    except Exception as e:
        print(f"Error retrieving MinIO info: {e}")
        sys.exit(3)  # Exit with UNKNOWN status in case of error


def check_cluster_mode(minio_info):
    """Check the global cluster mode and return a status."""
    cluster_mode = minio_info['info']['mode']
    cluster_mode_string = f'cluster: {cluster_mode}'
    status = 0 if cluster_mode == 'online' else 2  # Critical if not 'online'
    return cluster_mode_string, status


def check_server_and_drives(minio_info):
    """Check the health of servers and their drives."""
    server_state_string = ''
    drive_state_string = ''
    status = 0  # Default to OK

    for server in minio_info['info']['servers']:
        server_state_string += f" {server['endpoint']} : {server['state']}"
        if server['state'] != 'online':
            status = max(status, 2)  # Critical if server not online

        for drive in server['drives']:
            drive_endpoint = drive['endpoint'].replace('https://', '')
            drive_extra_info = ''

            if drive['state'] != 'ok' or 'healing' in drive:
                # Drive not healthy: assume WARNING status
                status = max(status, 1)
                if 'healing' in drive:
                    drive_extra_info = '(healing)'
                drive_state_string += f" {drive_endpoint} : {drive['state']} {drive_extra_info}"

    return server_state_string, drive_state_string, status


if __name__ == "__main__":
    minio_info = get_minio_info()

    # Check cluster mode
    cluster_mode_string, status = check_cluster_mode(minio_info)

    # Check server and drives
    server_state_string, drive_state_string, component_status = check_server_and_drives(minio_info)
    status = max(status, component_status)

    # Output status
    print(f"{status} MinIO - {STATUS_STRINGS[status]} - {cluster_mode_string} {server_state_string} {drive_state_string}")

    # Check Buckets Occupation
    buckets = get_buckets()
    for bucket in buckets:
        bucket_status, bucket_size, bucket_quota, bucket_usage_percentage, bucket_objects = check_bucket(bucket)
        if bucket_size is not None:
            if not bucket_quota and bucket_size:
                perf_data = f"used={bucket_size:.2f}GiB|objects={bucket_objects}"
                if bucket_objects > 0:
                    print(f"{bucket_status} Bucket_{bucket.replace('/', '')} {perf_data} Used: {bucket_size:.2f} GiB ({bucket_objects} Objects)")
                else:
                    print(f"{bucket_status} Bucket_{bucket.replace('/', '')} {perf_data} Used: {bucket_size:.2f} GiB")
            else:
                perf_data = f"used={bucket_size:.2f}GiB;{bucket_quota * 0.8:.2f}GiB;{bucket_quota * 0.9:.2f}GiB;0GiB;{bucket_quota:.2f}GiB|objects={bucket_objects}"
                if bucket_objects > 0:
                    print(f"{bucket_status} Bucket_{bucket.replace('/', '')} {perf_data} Used: {bucket_usage_percentage:.2f}% - {bucket_size:.2f} GiB of {bucket_quota:.2f} GiB ({bucket_objects} Objects)")
                else:
                    print(f"{bucket_status} Bucket_{bucket.replace('/', '')} {perf_data} Used: {bucket_usage_percentage:.2f}% - {bucket_size:.2f} GiB of {bucket_quota:.2f} GiB")
        else:
            print(f"{bucket_status} Bucket_{bucket.replace('/', '')}")

    exit(status)
