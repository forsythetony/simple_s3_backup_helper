#!/usr/bin/env python3

import argparse
import boto3
import logging as log
import os
import shutil
import time
import yaml

from send2trash import send2trash

TEMP_DIR_NAME = "temp"

BACKUP_PREFIX = "backup"

WASABI_CREDENTIALS = None
CONFIG = None
S3 = None

TRASH_BACKUP_FOLDER = False

#
#   SETUP
#
def setup():
    setup_config()
    setup_secrets()
    setup_logging()
    setup_argument_parsing()

    setup_temp_folder()

    load_s3()

def setup_temp_folder():
    temp_folder_path = build_local_path(TEMP_DIR_NAME)

    create_directory(temp_folder_path)

def setup_config():

    global CONFIG

    with open('config.yml') as file:
        CONFIG = yaml.safe_load(file)

def setup_secrets():
    
    global WASABI_CREDENTIALS

    with open('secrets/wasabi_credentials.yml') as file:
        WASABI_CREDENTIALS = yaml.safe_load(file)

def setup_logging():
    logging_format = "%(asctime)s: %(message)s"
    log.basicConfig(
        format=logging_format,
        level=log.DEBUG,
        datefmt="%H:%M:%S"
    )

def setup_argument_parsing():
    parser = argparse.ArgumentParser(
        description="A simple program to backup a folder and shoot it off to the cloud 🌩"
    )

    parser.add_argument(
        '--trashBackupFolder',
        '-t',
        help='If set the folder being backed up will be moved to the trash after upload',
        dest='trash_backup_folder',
        action='store_true',
        required=False,
        default=False
    )

    args = parser.parse_args()

    configure_globals(args.trash_backup_folder)    

def configure_globals(trash_backup_folder):

    global TRASH_BACKUP_FOLDER

    TRASH_BACKUP_FOLDER = trash_backup_folder


def load_s3():

    global S3

    S3 = boto3.resource(
        's3',
        endpoint_url=CONFIG['wasabi']['bucket_endpoint'],
        aws_access_key_id = WASABI_CREDENTIALS['access_key_id'],
        aws_secret_access_key = WASABI_CREDENTIALS['secret_key']
    )

#
#   END SETUP
#

#
#   TESTING FUNCTIONS
#
def list_items_in_bucket():
    
    backups_bucket = S3.Bucket(CONFIG['wasabi']['archive_bucket_name'])

    for obj in backups_bucket.objects.all():
        log.info(obj)
    

#
#   END TESTING FUNCTIONS
#

#
#   HELPER FUNCTIONS
#
def script_directory():
    """
    :returns: The directory that the script is currently running
    in.
    :rtype: str
    """
    return os.path.dirname(os.path.realpath(__file__))

def build_local_path(final_path_component):
    """Will build a path using the location of the python script
    as the root

    :param final_path_component: The path component that you want to
        located alongside the current script

    :returns: A full path to an item or directy rooted in the same folder
        as the current running script.
    :rtype: str
    """
    return os.path.join(script_directory(), final_path_component)

def create_directory(dir_path):
    """Creates a directory if it does not already exist

    :param dir_path: The path the the directory being created
    """

    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    
def find_latest_modified_directory(paths):
    """Given a collection of paths will return the one that
    has been most recently modified

    :param paths: A list of paths (type `str`)

    :returns: Path to the most recently modified directory
    :rtype: str
    """

    ret_path = None
    newest_timestamp = None

    for p in paths:
        modified_timestamp = os.path.getmtime(p)

        if newest_timestamp is None or modified_timestamp > newest_timestamp:
            newest_timestamp = modified_timestamp
            ret_path = p

    return ret_path

def find_backup_folder():
    """Finds the directory path of the backup folder

    Will look in the directory specified by CONFIG->machine->backup_folder_path.
    Always chooses the most recently created directory.
    
    :raises: 
        :Error: If a valid path cannot be found.
    
    :returns: Path to the directory to be zipped up and uploaded
    :rtype: str
    """
    
    backup_parent_dir = CONFIG['machine']['backup_folder_path']
    backup_folders = [f.path for f in os.scandir(backup_parent_dir) if f.is_dir()]

    if len(backup_folders) == 0:
        raise Exception(f"Failed to find any backup folders in {backup_parent_dir}")

    return find_latest_modified_directory(backup_folders)

def create_backup_name():
    """Creates name to use for backup.

    Ex:
    backup_20120515-155045

    :returns: Name of backup containing the current timestamp.
    :rtype: str
    """
    timestamp = time.strftime("%Y%m%d-%H%M%S")

    return f"{BACKUP_PREFIX}_{timestamp}"

def upload_zipped_folder(backup_base_name, zipped_file_path):
    
    bucket_name = CONFIG['wasabi']['archive_bucket_name']
    s3_key = f"{CONFIG['machine']['name']}/{backup_base_name}.zip"

    log.info(f"Attempting to upload file to bucket {bucket_name} with key {s3_key}")

    S3.meta.client.upload_file(
        zipped_file_path, 
        bucket_name,
        s3_key
    )

def clean_temp_folder():
    """Clears out all files from the temp folder
    """
    temp_dir = build_local_path(TEMP_DIR_NAME)

    log.info(f"Deleting temp folder at {temp_dir}")

    #   Just to be safe let's only clear out the zip files
    zip_files = [f for f in os.listdir(temp_dir) if f.endswith('.zip')]

    for f in zip_files:
        os.remove(os.path.join(temp_dir, f))

def trash_backup_folder(backup_folder_path):
    """Will send the directory being backed up to the trash. This seems safer
    than just deleting it as we can do a double check to see if things were
    _actually_ uploaded.

    :param backup_folder_path: The path of the directory being backed up
    """
    log.info(f"🗑 Trashing the backup folder at {backup_folder_path}")
    
    send2trash(backup_folder_path)

def cleanup(backup_folder_path):
    """Clean up any temp files created and remove backup directory if
    flag is set

    :param backup_folder_path: The path of the directory being backed up
    """

    log.info("Cleaning things up 🧹")
    clean_temp_folder()

    if TRASH_BACKUP_FOLDER:
        trash_backup_folder(backup_folder_path)

def main():
    setup()
    backup_folder_path = find_backup_folder()
    
    backup_base_name = create_backup_name()

    log.info(f"Will attempt to backup following directory 👉 {backup_folder_path}")

    temp_zip_path = os.path.join(build_local_path(TEMP_DIR_NAME), backup_base_name)

    shutil.make_archive(temp_zip_path, 'zip', backup_folder_path)

    log.info(f"Placed the zipped directory here 👉 {temp_zip_path}")

    upload_zipped_folder(backup_base_name, f"{temp_zip_path}.zip")

    cleanup(backup_folder_path)

if __name__ == "__main__":
    main()
