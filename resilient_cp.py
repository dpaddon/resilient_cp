"""
Resiliently CP files to a mounted network drive
"""

import argparse
import json
import logging
import os
import subprocess
import time

from tqdm import tqdm

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level="DEBUG")


def resilient_copy(source,
                target,
                max_retries=5,
                source_json_path=None,
                limit=10000):
    """
    Use CP to transfer files in a resilient manner, without duplication

    Args:
        source             : source directory. Everything within this directory will be copied to target.
        target             : target directory.
        max_retries        : how many times to attempt to transfer each file
        source_json_path   : optional JSON containing flags of which files have been transferred
        limit              : bandwidth limit for scp in kbit/s
    Returns:
        0 if successful in transferring all files; else 1
    """

    # If a source JSON is provided, use this:
    if source_json_path:
        if not os.path.exists(source_json_path):
            logger.error("No file exists at: %s", source_json_path)
            return 1
        # Read it in as a dict
        with open(source_json_path) as source_json:
            files_dict = json.load(source_json)
            logger.info("%s files in list", len(files_dict))

    # If no source file list is provided, create one so we can refer to it later
    else:
        # The JSON will be saved as the absolute filepath
        source_json_path = f"{os.path.abspath(source).replace('/','_')}.json"
        files_dict = {}
        for root, dirs, files in os.walk(source, topdown=True):
            for name in files:
                files_dict[os.path.join(os.path.abspath(root), name)]=False

        logger.info("%s files in list", len(files_dict))

        logger.info("Writing file list to JSON at %s", source_json_path)
        with open(os.path.join(".",source_json_path), "w") as outfile:
            json.dump(files_dict, outfile)
    
    num_files_to_transfer = len([f for f in files_dict if files_dict[f]==False])
    logger.info("%s files left to transfer", num_files_to_transfer)

    # Iterate through the files and transfer if necessary
    with tqdm(total=num_files_to_transfer) as p_bar:
        for file in files_dict:

            # If we have not already transferred this file
            if not files_dict[file]:
                p_bar.update(1)
                relpath = os.path.relpath(file, source) 
                target_path = os.path.join(target, relpath)

                # Try to transfer the file up to max_retries times
                for i in range(max_retries):
                    try:
                        if i > 0:
                            logger.debug("Attempt number %s ...", i+1)

                        logger.debug("Transferring file %s to %s", file, target_path)

                        # If necessary, make the parent directories
                        target_head_path, target_tail_path = os.path.split(target_path)
                        target_head_path = target_head_path.replace(" ", "\ ")
                        if not os.path.exists(target_head_path):
                            subprocess.call(f"mkdir -p {target_head_path}", shell=True)

                        source_file_path = file.replace(" ", "\ ")
                        return_code = subprocess.check_output(f"scp -l {limit} {source_file_path} {target_head_path}", shell=True)

                        # If we haven't excepted out, assume the file transfer was successful
                        files_dict[file]=True

                        # Write the updated JSON in case we fail soon
                        with open(os.path.join(".",source_json_path), "w") as outfile:
                            json.dump(files_dict, outfile)

                        # Exit and proceed with the next file
                        break

                    # If it failed, log the reason and then try again
                    except subprocess.CalledProcessError as e:
                        logger.error(e.output)

                        # Test network connection - if the target path is not mounted, 
                        # we'll assume we've got a connection issue and sleep 15 mins
                        # if not os.path.ismount(target_path):
                        #     logger.warning("Connection to mounted drive failed")
                        #     logger.warning("Sleeping for 15 mins")
                        #     time.sleep(900)

    print("Done.")
    
    num_files_to_transfer = len([f for f in files_dict if files_dict[f]==False])

    if num_files_to_transfer == 0:
        logger.info("Finished, with all files transferred.")
        return 0
    
    else:
        logger.info("Finished, with %s files left to transfer.", num_files_to_transfer)
        return 1


if __name__=="__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--source", default=None, type=str)
    parser.add_argument("-t", "--target", default=None, type=str)
    parser.add_argument("-m", "--max_retries", default=2, type=int)
    parser.add_argument("-l", "--limit", default=10000, type=int)
    parser.add_argument("-j", "--source_json", default=None, type=str)

    args = parser.parse_args()

    SOURCE_DIR = args.source
    TARGET_DIR = args.target
    max_retries = args.max_retries
    source_json_path = args.source_json
    limit = args.limit

    resilient_copy(SOURCE_DIR, TARGET_DIR, max_retries, source_json_path, limit)
