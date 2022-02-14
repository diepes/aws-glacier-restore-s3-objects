#!/usr/bin/env python3
# Base From: https://stackoverflow.com/questions/20033651/how-to-restore-folders-or-entire-buckets-to-amazon-s3-from-glacier https://stackoverflow.com/users/3299397/kyle-bridenstine
# 2022-02 modified, add bucket as argument, put in github, print updates for large s3 thaw
import argparse
# import base64
# import json
import os
import sys
from datetime import datetime
# from pathlib import Path

import boto3

# import pymysql.cursors
# import yaml
from botocore.exceptions import ClientError

def reportStatuses(
    operation,
    type,
    successOperation,
    folders,
    restoreFinished,
    restoreInProgress,
    restoreNotRequestedYet,
    restoreStatusUnknown,
    skippedFolders,
):
    """
    reportStatuses gives a generic, aggregated report for all operations (Restore, Status, Download)
    """

    report = 'Status Report For "{}" Operation. Of the {} total {}, {} are finished being {}, {} have a restore in progress, {} have not been requested to be restored yet, {} reported an unknown restore status, and {} were asked to be skipped.'.format(
        operation,
        str(len(folders)),
        type,
        str(len(restoreFinished)),
        successOperation,
        str(len(restoreInProgress)),
        str(len(restoreNotRequestedYet)),
        str(len(restoreStatusUnknown)),
        str(len(skippedFolders)),
    )

    if (len(folders) - len(skippedFolders)) == len(restoreFinished):
        print(report)
        print("Success: All {} operations are complete".format(operation))
    else:
        if (len(folders) - len(skippedFolders)) == len(restoreNotRequestedYet):
            print(report)
            print("Attention: No {} operations have been requested".format(operation))
        else:
            print(report)
            print("Attention: Not all {} operations are complete yet".format(operation))


def status(foldersToRestore, restoreTTL, s3Bucket):

    s3 = boto3.resource("s3")

    folders = []
    skippedFolders = []

    # Read the list of folders to process
    with open(foldersToRestore, "r") as f:

        for rawS3Path in f.read().splitlines():

            folders.append(rawS3Path)

            maxKeys = 1000
            # Remove the S3 Bucket Prefix to get just the S3 Path i.e., the S3 Objects prefix and key name
            s3Path = removeS3BucketPrefixFromPath(rawS3Path, s3Bucket)

            # Construct an S3 Paginator that returns pages of S3 Object Keys with the defined prefix
            client = boto3.client("s3")
            paginator = client.get_paginator("list_objects")
            operation_parameters = {
                "Bucket": s3Bucket,
                "Prefix": s3Path,
                "MaxKeys": maxKeys,
            }
            page_iterator = paginator.paginate(**operation_parameters)

            pageCount = 0

            totalS3ObjectKeys = []
            totalS3ObjKeysRestoreFinished = []
            totalS3ObjKeysRestoreInProgress = []
            totalS3ObjKeysRestoreNotRequestedYet = []
            totalS3ObjKeysRestoreStatusUnknown = []

            # Iterate through the pages of S3 Object Keys
            for page in page_iterator:

                for s3Content in page["Contents"]:

                    s3ObjectKey = s3Content["Key"]

                    # Folders show up as Keys but they cannot be restored or downloaded so we just ignore them
                    if s3ObjectKey.endswith("/"):
                        continue

                    totalS3ObjectKeys.append(s3ObjectKey)

                    s3Object = s3.Object(s3Bucket, s3ObjectKey)

                    if s3Object.restore is None:
                        totalS3ObjKeysRestoreNotRequestedYet.append(s3ObjectKey)
                    elif "true" in s3Object.restore:
                        totalS3ObjKeysRestoreInProgress.append(s3ObjectKey)
                    elif "false" in s3Object.restore:
                        totalS3ObjKeysRestoreFinished.append(s3ObjectKey)
                    else:
                        totalS3ObjKeysRestoreStatusUnknown.append(s3ObjectKey)

                pageCount = pageCount + 1
                print(
                    f"{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}  ... progress {pageCount=} {len(totalS3ObjKeysRestoreNotRequestedYet)=} {len(totalS3ObjKeysRestoreInProgress)=} {len(totalS3ObjKeysRestoreFinished)=} "
                )

            # Report the total statuses for the folders
            reportStatuses(
                "restore folder " + rawS3Path,
                "files",
                "restored",
                totalS3ObjectKeys,
                totalS3ObjKeysRestoreFinished,
                totalS3ObjKeysRestoreInProgress,
                totalS3ObjKeysRestoreNotRequestedYet,
                totalS3ObjKeysRestoreStatusUnknown,
                [],
            )


def removeS3BucketPrefixFromPath(path, bucket):
    """
    removeS3BucketPrefixFromPath removes "s3a://<bucket name>" or "s3://<bucket name>" from the Path
    """

    s3BucketPrefix1 = "s3a://" + bucket + "/"
    s3BucketPrefix2 = "s3://" + bucket + "/"

    if path.startswith(s3BucketPrefix1):
        # remove one instance of prefix
        return path.replace(s3BucketPrefix1, "", 1)
    elif path.startswith(s3BucketPrefix2):
        # remove one instance of prefix
        return path.replace(s3BucketPrefix2, "", 1)
    else:
        return path


def restore(foldersToRestore, restoreTTL, s3Bucket):
    """
    restore initiates a restore request on one or more folders
    """

    print("Restore Operation")

    s3 = boto3.resource("s3")
    bucket = s3.Bucket(s3Bucket)

    folders = []
    skippedFolders = []

    # Read the list of folders to process
    with open(foldersToRestore, "r") as f:

        for rawS3Path in f.read().splitlines():

            folders.append(rawS3Path)

            # Skip folders that are commented out of the file
            if "#" in rawS3Path:
                print(
                    "Skipping this folder {} since it's commented out with #".format(
                        rawS3Path
                    )
                )
                folders.append(rawS3Path)
                continue
            else:
                print("Restoring folder {}".format(rawS3Path))

            maxKeys = 1000
            # Remove the S3 Bucket Prefix to get just the S3 Path i.e., the S3 Objects prefix and key name
            s3Path = removeS3BucketPrefixFromPath(rawS3Path, s3Bucket)

            print(
                "s3Bucket={}, s3Path={}, maxKeys={}".format(s3Bucket, s3Path, maxKeys)
            )

            # Construct an S3 Paginator that returns pages of S3 Object Keys with the defined prefix
            client = boto3.client("s3")
            paginator = client.get_paginator("list_objects")
            operation_parameters = {
                "Bucket": s3Bucket,
                "Prefix": s3Path,
                "MaxKeys": maxKeys,
            }
            page_iterator = paginator.paginate(**operation_parameters)

            pageCount = 0

            totalS3ObjectKeys = []
            totalS3ObjKeysRestoreFinished = []
            totalS3ObjKeysRestoreInProgress = []
            totalS3ObjKeysRestoreNotRequestedYet = []
            totalS3ObjKeysRestoreStatusUnknown = []

            # Iterate through the pages of S3 Object Keys
            for page in page_iterator:

                print("Processing S3 Key Page {}".format(str(pageCount)))

                s3ObjectKeys = []
                s3ObjKeysRestoreFinished = []
                s3ObjKeysRestoreInProgress = []
                s3ObjKeysRestoreNotRequestedYet = []
                s3ObjKeysRestoreStatusUnknown = []

                for s3Content in page["Contents"]:

                    print("Processing S3 Object Key {}".format(s3Content["Key"]))

                    s3ObjectKey = s3Content["Key"]

                    # Folders show up as Keys but they cannot be restored or downloaded so we just ignore them
                    if s3ObjectKey.endswith("/"):
                        print(
                            "Skipping this S3 Object Key because it's a folder {}".format(
                                s3ObjectKey
                            )
                        )
                        continue

                    s3ObjectKeys.append(s3ObjectKey)
                    totalS3ObjectKeys.append(s3ObjectKey)

                    s3Object = s3.Object(s3Bucket, s3ObjectKey)

                    print(
                        "{} - {} - {}".format(
                            s3Object.key, s3Object.storage_class, s3Object.restore
                        )
                    )

                    # Ensure this folder was not already processed for a restore
                    if s3Object.restore is None:

                        restore_response = bucket.meta.client.restore_object(
                            Bucket=s3Object.bucket_name,
                            Key=s3Object.key,
                            RestoreRequest={"Days": restoreTTL},
                        )

                        print("Restore Response: {}".format(str(restore_response)))

                        # Refresh object and check that the restore request was successfully processed
                        s3Object = s3.Object(s3Bucket, s3ObjectKey)

                        print(
                            "{} - {} - {}".format(
                                s3Object.key, s3Object.storage_class, s3Object.restore
                            )
                        )

                        if s3Object.restore is None:
                            s3ObjKeysRestoreNotRequestedYet.append(s3ObjectKey)
                            totalS3ObjKeysRestoreNotRequestedYet.append(s3ObjectKey)
                            print("%s restore request failed" % s3Object.key)
                            # Instead of failing the entire job continue restoring the rest of the log tree(s)
                            # raise Exception("%s restore request failed" % s3Object.key)
                        elif "true" in s3Object.restore:
                            print(
                                "The request to restore this file has been successfully received and is being processed: {}".format(
                                    s3Object.key
                                )
                            )
                            s3ObjKeysRestoreInProgress.append(s3ObjectKey)
                            totalS3ObjKeysRestoreInProgress.append(s3ObjectKey)
                        elif "false" in s3Object.restore:
                            print(
                                "This file has successfully been restored: {}".format(
                                    s3Object.key
                                )
                            )
                            s3ObjKeysRestoreFinished.append(s3ObjectKey)
                            totalS3ObjKeysRestoreFinished.append(s3ObjectKey)
                        else:
                            print(
                                "Unknown restore status ({}) for file: {}".format(
                                    s3Object.restore, s3Object.key
                                )
                            )
                            s3ObjKeysRestoreStatusUnknown.append(s3ObjectKey)
                            totalS3ObjKeysRestoreStatusUnknown.append(s3ObjectKey)

                    elif "true" in s3Object.restore:
                        print(
                            "Restore request already received for {}".format(
                                s3Object.key
                            )
                        )
                        s3ObjKeysRestoreInProgress.append(s3ObjectKey)
                        totalS3ObjKeysRestoreInProgress.append(s3ObjectKey)
                    elif "false" in s3Object.restore:
                        print(
                            "This file has successfully been restored: {}".format(
                                s3Object.key
                            )
                        )
                        s3ObjKeysRestoreFinished.append(s3ObjectKey)
                        totalS3ObjKeysRestoreFinished.append(s3ObjectKey)
                    else:
                        print(
                            "Unknown restore status ({}) for file: {}".format(
                                s3Object.restore, s3Object.key
                            )
                        )
                        s3ObjKeysRestoreStatusUnknown.append(s3ObjectKey)
                        totalS3ObjKeysRestoreStatusUnknown.append(s3ObjectKey)

                # Report the statuses per S3 Key Page
                reportStatuses(
                    "folder-" + rawS3Path + "-page-" + str(pageCount),
                    "files in this page",
                    "restored",
                    s3ObjectKeys,
                    s3ObjKeysRestoreFinished,
                    s3ObjKeysRestoreInProgress,
                    s3ObjKeysRestoreNotRequestedYet,
                    s3ObjKeysRestoreStatusUnknown,
                    [],
                )

                pageCount = pageCount + 1

            if pageCount > 1:
                # Report the total statuses for the files
                reportStatuses(
                    "restore-folder-" + rawS3Path,
                    "files",
                    "restored",
                    totalS3ObjectKeys,
                    totalS3ObjKeysRestoreFinished,
                    totalS3ObjKeysRestoreInProgress,
                    totalS3ObjKeysRestoreNotRequestedYet,
                    totalS3ObjKeysRestoreStatusUnknown,
                    [],
                )


def displayError(operation, exc):
    """
    displayError displays a generic error message for all failed operation's returned exceptions
    """

    print(
        'Error! Restore{} failed. Please ensure that you ran the following command "aws sso login" and "export AWS_PROFILE=xyz" before executing this program. Error: {}'.format(
            operation, exc
        )
    )


def main(operation, foldersToRestore, restoreTTL, s3Bucket):
    """
    main The starting point of the code that directs the operation to it's appropriate workflow
    """

    print(
        "{} Starting log_migration_restore.py with operation={} foldersToRestore={} restoreTTL={} Day(s)".format(
            str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            operation,
            foldersToRestore,
            str(restoreTTL),
        )
    )

    if operation == "restore":
        try:
            restore(foldersToRestore, restoreTTL, s3Bucket)
        except Exception as exc:
            displayError("", exc)
    elif operation == "status":
        try:
            status(foldersToRestore, restoreTTL, s3Bucket)
        except Exception as exc:
            displayError("-Status-Check", exc)
    else:
        raise Exception(
            "%s is an invalid operation. Please choose either 'restore' or 'status'"
            % operation
        )


def check_operation(operation):
    """
    check_operation validates the runtime input arguments
    """

    if operation is None or (
        str(operation) != "restore"
        and str(operation) != "status"
        and str(operation) != "download"
    ):
        raise argparse.ArgumentTypeError(
            "%s is an invalid operation. Please choose either 'restore' or 'status' or 'download'"
            % operation
        )
    return str(operation)


# To run use sudo python3 /home/ec2-user/recursive_restore.py -- restore
# -l /home/ec2-user/folders_to_restore.csv
if __name__ == "__main__":

    # Form the argument parser.
    parser = argparse.ArgumentParser(
        description="Restore s3 folders from archival using 'restore' or check on the restore status using 'status'"
    )

    parser.add_argument(
        "operation",
        type=check_operation,
        help="Please choose either 'restore' to restore the list of s3 folders or 'status' to see the status of a restore on the list of s3 folders",
    )

    parser.add_argument(
        "-l",
        "--foldersToRestore",
        type=str,
        default="~/git/tools-aws/folders_to_restore.csv",
        required=False,
        help="The location of the file containing the list of folders to restore. Put one folder on each line.",
    )

    parser.add_argument(
        "-t",
        "--restoreTTL",
        type=int,
        default=14,
        required=False,
        help="The number of days you want the filess to remain restored/unarchived. After this period the logs will automatically be rearchived.",
    )

    parser.add_argument("--s3Bucket", required=True, help="S3 bucket to operate on.")

    args = parser.parse_args()
    sys.exit(
        main(
            args.operation,
            os.path.expanduser(args.foldersToRestore),
            args.restoreTTL,
            args.s3Bucket,
        )
    )
