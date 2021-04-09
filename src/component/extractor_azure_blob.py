import csv  # noqa
import glob
import json
import logging
import os
import re  # noqa
import sys

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from kbc.env_handler import KBCEnvHandler
from kbc.result import KBCTableDef  # noqa
from kbc.result import ResultWriter  # noqa

# configuration variables
KEY_ACCOUNT_NAME = 'account_name'
KEY_ACCOUNT_KEY = '#account_key'
KEY_CONTAINER_NAME = 'container_name'
KEY_FILES = 'files'
KEY_CONNECTION_STRING = '#connection_string'

KEY_DEBUG = 'debug'
ACCOUNT_GROUP = [KEY_ACCOUNT_NAME, KEY_ACCOUNT_KEY, KEY_CONNECTION_STRING]

MANDATORY_PARS = [
    ACCOUNT_GROUP,
    KEY_CONTAINER_NAME,
    KEY_FILES
]
MANDATORY_IMAGE_PARS = []

# Default Table Output Destination
DEFAULT_TABLE_SOURCE = "/data/in/tables/"
DEFAULT_TABLE_DESTINATION = "/data/out/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/files/"
DEFAULT_FILE_SOURCE = "/data/in/files/"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-8s : [line:%(lineno)3s] %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")

# Disabling list of libraries you want to output in the logger
disable_libraries = [
    'azure.storage.common.storageclient'
]
for library in disable_libraries:
    logging.getLogger(library).disabled = True

APP_VERSION = '0.0.1'


class ExtractorAzureBlobService(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS, debug=debug)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config()
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.error(e)
            exit(1)

    def validate_config_params(self, params):
        """
        Validating if input configuration contain everything needed
        """

        # Credentials Conditions
        # Validate if config is blank
        if params == {}:
            logging.error(
                'Configurations are missing. Please configure your component.')
            sys.exit(1)

        # Validate if the configuration is empty
        empty_config = {
            KEY_ACCOUNT_NAME: '',
            KEY_ACCOUNT_KEY: '',
            KEY_CONTAINER_NAME: '',
            KEY_CONNECTION_STRING: '',
            KEY_FILES: []
        }
        if params == empty_config:
            logging.error(
                'Configurations are missing. Please configure your component.')
            sys.exit(1)

        # Validating config parameters
        if (params[KEY_ACCOUNT_NAME] != '' and params[KEY_ACCOUNT_KEY] != '') or params[KEY_CONNECTION_STRING] != '':
            logging.error(
                "Credentials missing: Account Name, Account Key, Connection String ...")
            sys.exit(1)
        if params[KEY_CONTAINER_NAME] == '':
            logging.error(
                "Blob Container name is missing, check your configuration.")
            sys.exit(1)
        if len(params[KEY_FILES]) == 0:
            logging.error("Blob files configurations are missing." +
                          "Please configuration what Blob files you would like to extract.")
            sys.exit(1)

        # Validating inputs in Blob files configuration
        for file in params[KEY_FILES]:
            if file['file_name'] == '' or file['storage'] == '':
                logging.error(
                    'Blob files configuration cannot be empty: Name, Storage Name')
                sys.exit(1)

    def validate_blob_container(self, blob_obj, container_name):
        """
        Validating if input container exists in the Blob Storage
        """

        # List all containers for this account
        # & Determine if the input container is available
        # & Validate if the entered account has the right credentials and privileges
        try:
            container_generator = blob_obj._list_containers()
        except Exception:
            logging.error(
                'Authorization Error. Please validate your input credentials and account privileges.')
            sys.exit(1)

        list_of_containers = []
        for i in container_generator:
            list_of_containers.append(i.name)
        logging.info("Available Containers: {}".format(list_of_containers))
        if container_name not in list_of_containers:
            logging.error(
                "Container does not exist: {}".format(container_name))
            logging.error("Please validate your Blob Container.")
            sys.exit(1)

        return list_of_containers

    def produce_manifest(self, storage, columns, primary_key, incremental):
        """
        Dummy function to return header per file type.
        """

        file = "/data/out/tables/" + str(storage) + ".manifest"
        manifest_template = {
            "incremental": bool(incremental),
            "primary_key": primary_key,
            "columns": columns
        }

        manifest = manifest_template

        try:
            with open(file, 'w') as file_out:
                json.dump(manifest, file_out)
                logging.info(
                    "Output manifest file [{}] produced.".format(storage))
        except Exception as e:
            logging.error("Could not produce output file manifest.")
            logging.error(e)

        return

    def clean_csv(self, source, destination):
        """
        Removing header from source file and output as sliced file
        """

        with open(source, 'r') as f:
            with open(destination, 'w') as f1:
                header = next(f)
                for line in f:
                    f1.write(line)

        remove_these = ['\n', '\"']
        for i in remove_these:
            header = header.replace(i, '')
        header = header.split(",")

        return header

    def create_path_skeleton(self, file_list):
        """
        Creating a temp file structure to use glob
        """

        logging.info("Building Skeleton Structure...")
        for file in file_list:
            # Path configuration
            file_breakup = file.split('/')
            seperator = '/'
            path_name = seperator.join(file_breakup[:-1])
            if not os.path.exists(DEFAULT_TABLE_SOURCE + path_name):
                os.makedirs(DEFAULT_TABLE_SOURCE + path_name)

            # File Configuration
            with open(DEFAULT_TABLE_SOURCE + file, 'w') as b:
                pass
            b.close()

        return

    def run(self):
        """
        Main execution code
        """

        params = self.cfg_params  # noqac

        # Get proper list of tables
        account_name = params.get(KEY_ACCOUNT_NAME)
        account_key = params.get(KEY_ACCOUNT_KEY)
        container_name = params.get(KEY_CONTAINER_NAME)
        connection_string = params.get(KEY_CONNECTION_STRING)
        files = params.get(KEY_FILES)

        """
        Azure Blob Storage
        """
        # Create the BlockBlockService that is used to call the Blob service for the storage account
        block_blob_service = BlobServiceClient.from_connection_string(
            account_name=account_name, account_key=account_key, connection_string=connection_string)

        # Validate input container name
        self.validate_blob_container(blob_obj=block_blob_service, container_name=container_name)

        # List all Blobs from the specified containers
        # & Determine if the files listed in input is available
        # blob_generator = block_blob_service.list_blob_names(container_name)
        blob_generator = block_blob_service.list_blobs(container_name)
        list_of_blob = []
        for i in blob_generator:
            list_of_blob.append(i.name)
        logging.info("Available Blobs in {}: {}".format(
            container_name, list_of_blob))

        # Mock folder structure in Blob Storage
        self.create_path_skeleton(list_of_blob)

        for file in files:
            # Validate if input contains folder path
            qualified_files = []
            pure_dump = False  # Checking whether or not the files are CSV
            for name in glob.glob(DEFAULT_TABLE_SOURCE + file["file_name"]):
                if name.endswith(".csv"):
                    qualified_files.append(
                        name.replace(DEFAULT_TABLE_SOURCE, ''))
            logging.info("Qualified File [{0}]: {1}".format(
                file["file_name"], qualified_files))
            output_name = file["storage"]
            incremental = file["incremental"]
            primary_key = file["primary_key"]
            headers = []

            if len(qualified_files) != 0:
                for file_name in qualified_files:
                    # if '/' in file_name and folder_bool:
                    logging.info("Downloading {}...".format(file_name))
                    # Create target directory for slice files
                    try:
                        if pure_dump:
                            # os.mkdir(DEFAULT_FILE_DESTINATION + output_name)
                            os.mkdir(DEFAULT_FILE_DESTINATION + output_name)
                        else:
                            os.mkdir(DEFAULT_TABLE_DESTINATION + output_name)
                    except FileExistsError:
                        pass
                    # block_blob_service.get_blob_to_path(
                    #    container_name, file_name, DEFAULT_TABLE_SOURCE+file_name)

                    blob_file_path = file_name.split('/')
                    blob_file_name = blob_file_path[-1]
                    if not pure_dump:
                        """ CSV Files """
                        logging.debug("[{0}]File Output Path: {1}".format(
                            blob_file_name, DEFAULT_TABLE_DESTINATION + output_name + '/' + blob_file_name))
                        block_blob_service.get_blob_to_path(
                            container_name, file_name, DEFAULT_TABLE_SOURCE + file_name)
                        # container_name, file_name, DEFAULT_TABLE_DESTINATION+output_name+'/'+file_name)
                        temp_header = self.clean_csv(
                            # DEFAULT_TABLE_SOURCE+'/'+file_name, DEFAULT_TABLE_DESTINATION+output_name+'/'+file_name)
                            DEFAULT_TABLE_SOURCE + '/' + file_name, DEFAULT_TABLE_DESTINATION + output_name + '/'
                            + blob_file_name)

                        if len(headers) == 0:
                            headers = temp_header
                        elif len(temp_header) != len(headers):
                            logging.error(
                                "There are misaligned columns: [{}]".format(file_name))

                    else:
                        """ Other file format """
                        logging.debug("[{0}]File Output Path: {1}".format(
                            blob_file_name, DEFAULT_FILE_DESTINATION + output_name + '/' + blob_file_name))
                        blob_file_path = file_name.split('/')
                        blob_file_name = blob_file_path[-1]
                        block_blob_service.get_blob_to_path(
                            container_name, file_name, DEFAULT_FILE_DESTINATION + output_name + '/' + blob_file_name)

            else:
                logging.error("[{0}] does not exist in container [{1}]".format(
                    file["file_name"], container_name))
                sys.exit(1)

            # Output manifest for the sliced file
            if not pure_dump:
                self.produce_manifest(output_name, headers,
                                      primary_key, incremental)

        logging.info("Extraction finished")

        return


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug = sys.argv[1]
    else:
        debug = True
    comp = ExtractorAzureBlobService(debug)
    comp.run()
