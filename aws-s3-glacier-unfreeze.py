#!/usr/bin/env python3
# Base From: https://stackoverflow.com/questions/20033651/how-to-restore-folders-or-entire-buckets-to-amazon-s3-from-glacier https://stackoverflow.com/users/3299397/kyle-bridenstine
# 2022-02 modified, add bucket as argument, put in github, print updates for large s3 thaw
import argparse

# import base64
# import json
import os
import sys
from datetime import datetime
import time

# from pathlib import Path

import boto3

# import pymysql.cursors
# import yaml
from botocore.exceptions import ClientError


import asyncio
import concurrent.futures
import functools


def reportStatuses(
    operation,
    type,
    successOperation,
    totals,
    # restoreFinished,
    # restoreInProgress,
    # restoreNotRequestedYet,
    # restoreStatusUnknown,
    # skippedFolders,
    outputFileBase,
    outputMsg="",
):
    """
    reportStatuses gives a generic, aggregated report for all operations (Restore, Status, Download)
    """

    report = 'Status Report For "{}" Operation. Of the {} total {}, {} are finished being {}, {} have a restore in progress, {} have not been requested to be restored yet, {} reported an unknown restore status, and {} were asked to be skipped.'.format(
        operation,
        str(len(totals["Keys"])),
        type,
        str(len(totals["KeysRestoreFinished"])),
        successOperation,
        str(len(totals["KeysRestoreInProgress"])),
        str(len(totals["KeysRestoreNotRequestedYet"])),
        str(len(totals["KeysRestoreStatusUnknown"])),
        str(len(totals["KeysSkippedFolders"])),
    )

    if (len(totals["Keys"]) - len(totals["KeysSkippedFolders"])) == len(
        totals["KeysRestoreFinished"]
    ):
        print(report)
        print("Success: All {} operations are complete".format(operation))
    else:
        if (len(totals["Keys"]) - len(totals["KeysSkippedFolders"])) == len(
            totals["KeysRestoreNotRequestedYet"]
        ):
            print(report)
            print("Attention: No {} operations have been requested".format(operation))
        else:
            print(report)
            print("Attention: Not all {} operations are complete yet".format(operation))
    if outputFileBase:  # Only write to file if var set.
        with open(
            f"{outputFileBase}.restoreInProgress.csv", mode="at", encoding="utf-8"
        ) as myfile:
            myfile.write(f"# {outputMsg}\n")
            myfile.write("\n".join(totals["KeysRestoreInProgress"]))


async def status(foldersToRestore, restoreTTL, s3Bucket, outputFileBase):

    s3 = boto3.resource("s3")
    s3_Object = aio(s3.Object)

    folders = []
    time_start = time.monotonic()
    # Read the list of folders to process
    with open(foldersToRestore, "r") as f:

        for rawS3Path in f.read().splitlines():

            folders.append(rawS3Path)

            maxKeys = 1000
            # Remove the S3 Bucket Prefix to get just the S3 Path i.e., the S3 Objects prefix and key name
            s3Path = removeS3BucketPrefixFromPath(rawS3Path, s3Bucket)

            # Construct an S3 Paginator that returns pages of S3 Object Keys with the defined prefix
            client = boto3.client("s3")
            #    s3 = boto3.client('s3')
            # get_object = aio(s3.get_object)
            paginator = client.get_paginator("list_objects")
            operation_parameters = {
                "Bucket": s3Bucket,
                "Prefix": s3Path,
                "MaxKeys": maxKeys,
            }
            page_iterator = paginator.paginate(**operation_parameters)

            async def processPage(page, total, pageCount):
                time_start_page = time.monotonic()
                count_page_files = 0
                start_ObjectKeys = len(total["Keys"]) - 1
                for s3Content in page["Contents"]:
                    s3ObjectKey = s3Content["Key"]
                    count_page_files += 1
                    # Folders show up as Keys but they cannot be restored or downloaded so we just ignore them
                    if s3ObjectKey.endswith("/"):
                        total["KeysSkippedFolders"].append(s3ObjectKey)
                        print(
                            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ... skip dir {s3ObjectKey}"
                            + f" {pageCount=} {len(total['KeysRestoreNotRequestedYet'])=}"
                            + f" {len(total['KeysRestoreFinished'])=} {time.monotonic()-time_start_page:0.2f}s {count_page_files/(time.monotonic()-time_start_page):0.0f}obj/sec"
                            + f" {(len(total['Keys'])-start_ObjectKeys)/(time.monotonic()-time_start_page):0.0f}objAll/sec                      ",
                            end="\n",
                        )
                        continue

                    total["Keys"].append(s3ObjectKey)

                    # s3Object = s3.Object(s3Bucket, s3ObjectKey)
                    s3Object = await s3_Object(bucket_name=s3Bucket, key=s3ObjectKey)

                    if s3Object.restore is None:
                        total["KeysRestoreNotRequestedYet"].append(s3ObjectKey)
                    elif "true" in s3Object.restore:
                        total["KeysRestoreInProgress"].append(s3ObjectKey)
                    elif "false" in s3Object.restore:
                        total["KeysRestoreFinished"].append(s3ObjectKey)
                    else:
                        total["KeysRestoreStatusUnknown"].append(s3ObjectKey)

                print(
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ... progress {pageCount=} {len(total['KeysRestoreNotRequestedYet'])=} {len(total['KeysRestoreInProgress'])=} {len(total['KeysRestoreFinished'])=} "
                )

            pageCount = 0
            total = dict()
            total["KeysSkippedFolders"] = []
            total["Keys"] = []
            total["KeysRestoreFinished"] = []
            total["KeysRestoreInProgress"] = []
            total["KeysRestoreNotRequestedYet"] = []
            total["KeysRestoreStatusUnknown"] = []

            # Iterate through the pages of S3 Object Keys
            await asyncio.gather(
                *(processPage(page, total, i) for i, page in enumerate(page_iterator))
            )
            # for page in page_iterator:
            #     pageCount = pageCount + 1
            #     await processPage(page, total, pageCount)

            # Report the total statuses for the folders
            reportStatuses(
                "restore folder " + rawS3Path,
                "files",
                "restored",
                totals=total,
                outputFileBase=outputFileBase,
            )

            print(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  DONE"
                + f" {total['Keys']=} {len(total['KeysRestoreNotRequestedYet'])=}"
                + f" {len(total['KeysRestoreFinished'])=} {time.monotonic()-time_start:0.2f}s {total['Keys']/(time.monotonic()-time_start):0.0f}obj/sec                      ",
                end="\n",
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


async def restore_or_status(
    foldersToRestore, restoreTTL, s3Bucket, outputFileBase, operation
):
    """
    restore initiates a restore request on one or more folders
    """

    print(f"{operation} Operation")

    s3 = boto3.resource("s3")
    bucket = s3.Bucket(s3Bucket)
    s3_Object = aio(s3.Object)

    time_start = time.monotonic()
    folders = []

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
            elif operation == "restore":
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

            total = dict()
            total["KeysSkippedFolders"] = []
            total["Keys"] = []
            total["KeysRestoreFinished"] = []
            total["KeysRestoreInProgress"] = []
            total["KeysRestoreNotRequestedYet"] = []
            total["KeysRestoreStatusUnknown"] = []

            # Iterate through the pages of S3 Object Keys
            for page in page_iterator:
                time_start_page = time.monotonic()
                print("Processing S3 Key Page {}".format(str(pageCount)))

                s3Obj = dict()
                s3Obj["Keys"] = []
                s3Obj["KeysSkippedFolders"] = []
                s3Obj["KeysRestoreFinished"] = []
                s3Obj["KeysRestoreInProgress"] = []
                s3Obj["KeysRestoreNotRequestedYet"] = []
                s3Obj["KeysRestoreStatusUnknown"] = []

                for s3Content in page["Contents"]:

                    print("Processing S3 Object Key {}".format(s3Content["Key"]))

                    s3ObjectKey = s3Content["Key"]

                    # Folders show up as Keys but they cannot be restored or downloaded so we just ignore them
                    if s3ObjectKey.endswith("/"):
                        s3Obj["KeysSkippedFolders"].append(s3ObjectKey)
                        total["KeysSkippedFolders"].append(s3ObjectKey)
                        print(
                            "    Skipping this S3 Object Key because it's a folder {}".format(
                                s3ObjectKey
                            )
                        )
                        continue

                    s3Obj["Keys"].append(s3ObjectKey)
                    total["Keys"].append(s3ObjectKey)

                    # Get s3 object status
                    # s3Object = s3.Object(s3Bucket, s3ObjectKey)
                    s3Object = await s3_Object(bucket_name=s3Bucket, key=s3ObjectKey)
                    restore_requested = False

                    print(
                        "    {} - {} - {}".format(
                            s3Object.key, s3Object.storage_class, s3Object.restore
                        )
                    )

                    # Ensure this folder was not already processed for a restore
                    # If not request restore
                    if s3Object.restore is None and operation == "restore":
                        restore_response = bucket.meta.client.restore_object(
                            Bucket=s3Object.bucket_name,
                            Key=s3Object.key,
                            RestoreRequest={"Days": restoreTTL},
                        )
                        print("    Restore Response: {}".format(str(restore_response)))
                        # Refresh object and check that the restore request was successfully processed
                        # s3Object = s3.Object(s3Bucket, s3ObjectKey)
                        s3Object = await s3_Object(
                            bucket_name=s3Bucket, key=s3ObjectKey
                        )
                        restore_requested = True
                        print(
                            "    {} - {} - {}".format(
                                s3Object.key, s3Object.storage_class, s3Object.restore
                            )
                        )

                    # Final check of object restore status
                    if s3Object.restore is None:
                        s3Obj["KeysRestoreNotRequestedYet"].append(s3ObjectKey)
                        total["KeysRestoreNotRequestedYet"].append(s3ObjectKey)
                        if restore_requested:  # ToDo: error counter ?
                            print("    %s restore request failed" % s3Object.key)
                        # Instead of failing the entire job continue restoring the rest of the log tree(s)
                        # raise Exception("%s restore request failed" % s3Object.key)
                    elif "true" in s3Object.restore:
                        msg = (
                            "    This file has successfully been restored: {}"
                            if restore_requested
                            else "    Restore request already received for {}"
                        )
                        print(msg.format(s3Object.key))
                        s3Obj["KeysRestoreInProgress"].append(s3ObjectKey)
                        total["KeysRestoreInProgress"].append(s3ObjectKey)
                    elif "false" in s3Object.restore:
                        msg = (
                            "    This file has successfully been restored: {}"
                            if restore_requested
                            else "    This file has successfully been restored: {}"
                        )
                        print(msg.format(s3Object.key))
                        s3Obj["KeysRestoreFinished"].append(s3ObjectKey)
                        total["KeysRestoreFinished"].append(s3ObjectKey)
                    else:
                        print(
                            "    Unknown restore status ({}) for file: {}".format(
                                s3Object.restore, s3Object.key
                            )
                        )
                        s3Obj["KeysRestoreStatusUnknown"].append(s3ObjectKey)
                        total["KeysRestoreStatusUnknown"].append(s3ObjectKey)

                # Report the statuses per S3 Key Page
                reportStatuses(
                    "folder-" + rawS3Path + "-page-" + str(pageCount),
                    "files in this page",
                    "restored",
                    totals=s3Obj,
                    # s3ObjKeys,
                    # s3ObjKeysRestoreFinished,
                    # s3ObjKeysRestoreInProgress,
                    # s3ObjKeysRestoreNotRequestedYet,
                    # s3ObjKeysRestoreStatusUnknown,
                    outputFileBase=outputFileBase,
                )

                pageCount = pageCount + 1

            if pageCount > 1:
                # Report the total statuses for the files
                reportStatuses(
                    "restore-folder-" + rawS3Path,
                    "files",
                    "restored",
                    totals=total,
                    # total["Keys"],
                    # total["KeysRestoreFinished"],
                    # total["KeysRestoreInProgress"],
                    # total["KeysRestoreNotRequestedYet"],
                    # total["KeysRestoreStatusUnknown"],
                    outputFileBase=outputFileBase,
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


def aio(f):
    """From https://medium.com/@s.zeort/asynchronous-aws-s3-client-in-python-4f6b33829da6
    Wrap boto3 in async.
    s3 = boto3.client('s3')
    get_object = aio(s3.get_object)
    """
    loop = asyncio.get_running_loop()

    async def aio_wrapper(**kwargs):
        f_bound = functools.partial(f, **kwargs)
        # loop = asyncio.get_running_loop()
        # return await loop.run_in_executor(executor, f_bound)
        return await loop.run_in_executor(None, f_bound)

    return aio_wrapper


def main(operation, foldersToRestore, restoreTTL, s3Bucket, outputFileBase):
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

    if operation == "restore" or operation == "status":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            restore_or_status(
                foldersToRestore,
                restoreTTL,
                s3Bucket,
                outputFileBase=outputFileBase,
                operation=operation,
            )
        )
    elif operation == "status":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            status(foldersToRestore, restoreTTL, s3Bucket, outputFileBase)
        )
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
        description=r"""
            Restore s3 folders from archival/glacier using 'restore' or check on the restore status using 'status'.
            Will use the --foldersToRestore input file as base to create recoreds of files to be checked later
        """
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
            outputFileBase=os.path.expanduser(args.foldersToRestore),
        )
    )
