#!/usr/bin/env python3
# Base From: https://stackoverflow.com/questions/20033651/how-to-restore-folders-or-entire-buckets-to-amazon-s3-from-glacier https://stackoverflow.com/users/3299397/kyle-bridenstine
# 2022-02 modified, add bucket as argument, put in github, print updates for large s3 thaw
import cProfile
import argparse
from dataclasses import dataclass
from hashlib import new

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

import dataclasses
import json
import re

time_start = time.monotonic()


@dataclass
class class_totals:
    """Class to keep track of progress"""

    Keys: list = dataclasses.field(default_factory=list, repr=False)
    KeysSkippedFolders: list = dataclasses.field(default_factory=list, repr=False)
    KeysRestoreFinished: list = dataclasses.field(default_factory=list, repr=False)
    KeysRestoreInProgress: list = dataclasses.field(default_factory=list, repr=False)
    KeysRestoreNotRequestedYet: list = dataclasses.field(
        default_factory=list, repr=False
    )
    KeysRestoreStatusUnknown: list = dataclasses.field(default_factory=list, repr=False)
    KeysNotes: dict = dataclasses.field(default_factory=dict, repr=False)

    def __str__(self):
        return f"Keys[ Total:{len(self.Keys)} Folders:{len(self.KeysSkippedFolders)} Thawed:{len(self.KeysRestoreFinished)} InProgress:{len(self.KeysRestoreInProgress)} NotReq:{len(self.KeysRestoreNotRequestedYet)} Unk:{len(self.KeysRestoreStatusUnknown)} ]"

    def __repr__(self):
        return f"Keys[ Total:{len(self.Keys)} Folders:{len(self.KeysSkippedFolders)} Thawed:{len(self.KeysRestoreFinished)} InProgress:{len(self.KeysRestoreInProgress)} NotReq:{len(self.KeysRestoreNotRequestedYet)} Unk:{len(self.KeysRestoreStatusUnknown)} ]"


def add_totals(totals: class_totals, extratotals: class_totals):
    totals.Keys.extend(extratotals.Keys)
    totals.KeysSkippedFolders.extend(extratotals.KeysSkippedFolders)
    totals.KeysRestoreFinished.extend(extratotals.KeysRestoreFinished)
    totals.KeysRestoreInProgress.extend(extratotals.KeysRestoreInProgress)
    totals.KeysRestoreNotRequestedYet.extend(extratotals.KeysRestoreNotRequestedYet)
    totals.KeysRestoreStatusUnknown.extend(extratotals.KeysRestoreStatusUnknown)
    totals.KeysNotes.update(extratotals.KeysNotes)


####################################
def reportStatuses(
    operation,
    type,
    successOperation,
    totals: class_totals,
    outputFileBase,
    outputMsg="no outputMsg",
):
    """
    reportStatuses gives a generic, aggregated report for all operations (Restore, Status, Download)
    """

    report = (
        f'Status Report {successOperation} "{operation}". {type} total={len(totals.Keys)} = '
        f"finished={len(totals.KeysRestoreFinished)}, "
        f"InProgress={len(totals.KeysRestoreInProgress)}, "
        f"NotRequested={len(totals.KeysRestoreNotRequestedYet)}, "
        f"Unknown={len(totals.KeysRestoreStatusUnknown)}, "
        f"SkippedFolders={len(totals.KeysSkippedFolders)}, "
        f"Notes={len(totals.KeysNotes.keys())}"
    )

    if (len(totals.Keys) - len(totals.KeysSkippedFolders)) == len(
        totals.KeysRestoreFinished
    ):
        print(report)
        print(f"    Success: All {operation} operations are complete")
    else:
        if (len(totals.Keys) - len(totals.KeysSkippedFolders)) == len(
            totals.KeysRestoreNotRequestedYet
        ):
            print(report)
            print(f"    Attention: No {operation} operations have been requested")
        else:
            print(report)
            print(f"    Attention: Not all {operation} operations are complete yet")
    if outputFileBase:  # Only write to file if var set.
        with open(
            f"{outputFileBase}.restoreInProgress.csv", mode="at", encoding="utf-8"
        ) as myfile:
            print(
                f"DEBUG-output {len(totals.KeysRestoreInProgress)=} {totals.KeysRestoreInProgress=}"
            )
            myfile.write(f"# {outputMsg}\n")
            myfile.write(f"# {report}\n")
            myfile.write("\n".join(totals.KeysRestoreInProgress))
            myfile.write("\n")
            myfile.write(
                "\n".join([f"# {k} - {v}" for k, v in totals.KeysNotes.items()])
            )
            myfile.write("\n")


async def processPage(
    page,
    pageCount,
    rawS3Path,
    s3Bucket,
    outputFileBase,
    operation,
    restoreTTL=14,
) -> class_totals:
    """Takes page of s3 objects and checks or un-thaw's them, keeps totals for this page and returns them"""
    prefix = f"processPage p#{pageCount}"
    print(f"{prefix} {len(page['Contents'])=} started ...")
    time_start_page = time.monotonic()
    count_page_files = 0

    s3 = boto3.resource("s3")
    bucket = s3.Bucket(s3Bucket)
    s3_Object = aio(s3.Object)

    s3Obj = class_totals()

    for s3Content in page["Contents"]:
        # print(f"{prefix} {s3Obj=}")
        count_page_files += 1
        s3ObjectKey = s3Content["Key"]
        # Folders show up as Keys but they cannot be restored or downloaded so we just ignore them
        if s3ObjectKey.endswith("/"):
            s3Obj.KeysSkippedFolders.append(s3ObjectKey)
            s3Obj.Keys.append(s3ObjectKey)  # Total includes folders
            s3Obj.KeysNotes[s3ObjectKey] = f"Skipping folder {s3ObjectKey}"
            print(
                f"    {prefix} Skipping this S3 Object Key because it's a folder {s3ObjectKey}"
            )
            continue  # For loop, not included in Keys.
        s3Obj.Keys.append(s3ObjectKey)
        # Get s3 object status
        s3Object = await s3_Object(bucket_name=s3Bucket, key=s3ObjectKey)
        restore_requested = False
        msg = ""
        # Ensure this folder was not already processed for a restore
        # If not request restore
        if s3Object.restore is None and operation == "restore":
            restore_response = bucket.meta.client.restore_object(
                Bucket=s3Object.bucket_name,
                Key=s3Object.key,
                RestoreRequest={"Days": restoreTTL},
            )
            # msg += f" ReqRestore:{str(restore_response)=}"
            # Refresh object and check that the restore request was successfully processed
            # s3Object = s3.Object(s3Bucket, s3ObjectKey)
            s3Object = await s3_Object(bucket_name=s3Bucket, key=s3ObjectKey)
            restore_requested = True
            msg += f" ReqRestore:[{s3Object.restore}=]"
        # [{s3Object.restore}]
        # Final check of object restore status
        if s3Object.restore is None:
            s3Obj.KeysRestoreNotRequestedYet.append(s3ObjectKey)
            if restore_requested:  # ToDo: error counter ?
                msg += f" ERR:restore_request_failed"
            # Instead of failing the entire job continue restoring the rest of the log tree(s)
            # raise Exception("%s restore request failed" % s3Object.key)
        elif "true" in s3Object.restore:
            msg += (
                f" RESTORE:Success"
                if restore_requested
                else f" RESTORE:already_received"
            )
            s3Obj.KeysRestoreInProgress.append(s3ObjectKey)
        elif "false" in s3Object.restore:
            ## s3Object.restore='ongoing-request="false", expiry-date="Thu, 14 Apr 2022 00:00:00 GMT"'
            restore = re.sub(r"([\w-]+)=", r'"\1":', f"{{{s3Object.restore}}}")
            restore_dict = json.loads(restore)
            msg += (
                f" RESTORE:requested_and_done"
                if restore_requested
                else f" RESTORE:done expire=\"{restore_dict.get('expiry-date','No-expiry-date')}\""
            )
            s3Obj.KeysRestoreFinished.append(s3ObjectKey)
        else:
            msg += f" Unknown restore status ({s3Object.restore})"
            s3Obj.KeysRestoreStatusUnknown.append(s3ObjectKey)
        s3Obj.KeysNotes[s3ObjectKey] = msg

        runseconds = time.monotonic() - time_start_page
        # print(
        #     f"S3 Obj class:{s3Object.storage_class}"
        #     + f" stats #{pageCount} {runseconds:0.2f}s"
        #     + f" @{(count_page_files)/runseconds:0.1f}obj/sec"
        #     + f" Key:{s3Content['Key']} {msg}"
        # )

    runsecondsTotal = time.monotonic() - time_start
    # Report the statuses per S3 Key Page
    reportStatuses(
        operation="folder:" + rawS3Path + "-p#" + str(pageCount),
        type="files in this page",
        successOperation="restored",
        totals=s3Obj,
        outputFileBase=outputFileBase,
        outputMsg=f"page:{pageCount} objs:{len(s3Obj.Keys)} objsInProgress:{len(s3Obj.KeysRestoreInProgress)} in {runsecondsTotal:0.1f}sec",
    )
    # print(f"    {prefix} done.")
    return s3Obj


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

    folders = []
    total = class_totals()

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
                # folders.append(rawS3Path)
                continue
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
            activetasks = list()  # list of active tasks.

            async def asyncpages(max_pages=3):
                # Iterate through the pages of S3 Object Keys
                for pageCount, page in enumerate(page_iterator):
                    print(
                        "Starting Processing for S3 Keys Page {}".format(str(pageCount))
                    )
                    activetasks.append(
                        asyncio.create_task(
                            processPage(
                                page=page,
                                pageCount=pageCount,
                                rawS3Path=rawS3Path,
                                s3Bucket=s3Bucket,
                                outputFileBase=f"{outputFileBase}-{s3Path}-P{pageCount}",
                                operation=operation,
                                restoreTTL=restoreTTL,
                            ),
                            name=f"page{pageCount}",
                        )
                    )
                    if (
                        len(activetasks) >= max_pages
                    ):  # When max task, wait for oldest to finish
                        result = await asyncio.gather(activetasks.pop(0))
                        print(f"{result=}")
                # Wait for last tasks to finish.
                results = await asyncio.gather(*activetasks)
                for result in results:
                    add_totals(total, result)

            # Options2 threads
            def wrapper(coro):
                return asyncio.run(coro)

            async def threadpages(max_pages):
                with concurrent.futures.ThreadPoolExecutor(  # Options ThreadPool or ProcessPool
                    max_workers=max_pages
                ) as executor:
                    futures = dict()
                    for pageCount, page in enumerate(page_iterator):
                        futures[  # Use future as key
                            executor.submit(
                                wrapper,
                                processPage(
                                    page=page,
                                    pageCount=pageCount,
                                    rawS3Path=rawS3Path,
                                    s3Bucket=s3Bucket,
                                    outputFileBase=f"{outputFileBase}-{s3Path}-P{pageCount}",
                                    operation=operation,
                                    restoreTTL=restoreTTL,
                                ),
                            )
                        ] = {"pageCount": pageCount, "pageLen": len(page["Contents"])}

                    print(f"# All threads running ... waiting for them to finish")
                    for future in concurrent.futures.as_completed(
                        futures.keys(), timeout=60 * 10
                    ):
                        id = futures[future]  # pageCount, pageLen
                        print(
                            f"# Thread {id['pageCount']} finished, get result and add to total {future.running()=} {future.done()=}"
                        )
                        assert (
                            future.done() == True
                        ), f"Error got future back that is not done {id['pageCount']} {future=} ???"
                        result = future.result()
                        # result = await result_coroutine
                        print(
                            f"# DEBUG got result {time.ctime()}  {id=} {result=} {future=} {future.running()=} {future.done()=}"
                        )
                        add_totals(total, result)

            # Pick what to run
            # await asyncpages(max_pages=3)
            await threadpages(max_pages=40)

        # Report the total statuses for the files
        runseconds = time.monotonic() - time_start
        reportStatuses(
            operation=f"{operation} done in {runseconds:0.2f}s @{len(total.Keys)/runseconds:0.1f}obj/sec",
            type="files",
            successOperation="restored",
            totals=total,
            outputFileBase=outputFileBase,
            outputMsg="Final output - Done!",
        )

        print(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  DONE"
            + f" {len(total.Keys)=} {len(total.KeysRestoreNotRequestedYet)=}"
            + f" {len(total.KeysRestoreFinished)=} {time.monotonic()-time_start:0.2f}s {len(total.Keys)/(time.monotonic()-time_start):0.0f}obj/sec                      ",
            end="\n",
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
        default="~/git/tools-aws/aws-s3-glacier-restore-objects/folders_to_restore.csv",
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
    # sys.exit(
    # cProfile.run(
    main(
        args.operation,
        os.path.expanduser(args.foldersToRestore),
        args.restoreTTL,
        args.s3Bucket,
        outputFileBase=os.path.expanduser(args.foldersToRestore),
    )
    # )
