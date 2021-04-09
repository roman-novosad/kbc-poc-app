import logging
import os
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Callable

import backoff
import paramiko
from keboola.component import CommonInterface

MAX_RETRIES = 5

KEY_USER = 'user'
KEY_PASSWORD = '#pass'
KEY_HOSTNAME = 'hostname'
KEY_PORT = 'port'
KEY_REMOTE_PATH = 'path'
KEY_APPEND_DATE = 'append_date'
KEY_PRIVATE_KEY = '#private_key'

KEY_DEBUG = 'debug'
PASS_GROUP = [KEY_PRIVATE_KEY, KEY_PASSWORD]

REQUIRED_PARAMETERS = [KEY_USER, PASS_GROUP,
                       KEY_HOSTNAME, KEY_REMOTE_PATH, KEY_PORT]

REQUIRED_IMAGE_PARS = []

APP_VERSION = '1.0.0'


def get_local_data_path():
    return Path(__file__).resolve().parent.parent.joinpath('data').as_posix()


def get_data_folder_path():
    data_folder_path = None
    if not os.environ.get('KBC_DATADIR'):
        data_folder_path = get_local_data_path()
    return data_folder_path


class Component(CommonInterface):
    def __init__(self):
        # for easier local project setup
        data_folder_path = get_data_folder_path()
        super().__init__(data_folder_path=data_folder_path)
        try:
            self.validate_configuration(REQUIRED_PARAMETERS)
            self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        except ValueError as err:
            logging.exception(err)
            exit(1)
        if self.configuration.parameters.get(KEY_DEBUG):
            self.set_debug_mode()

    @staticmethod
    def set_debug_mode():
        logging.getLogger().setLevel(logging.DEBUG)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

    def run(self):
        '''
        Main execution code
        '''
        params = self.configuration.parameters
        pkey = self.get_private_key(params[KEY_PRIVATE_KEY])

        sftp, conn = self.connect_to_server(params[KEY_PORT],
                                            params[KEY_HOSTNAME],
                                            params[KEY_USER],
                                            params[KEY_PASSWORD],
                                            pkey)

        in_tables = self.get_input_tables_definitions()
        in_files_per_tag = self.get_input_file_definitions_grouped_by_tag_group(only_latest_files=True)
        in_files = [item for sublist in in_files_per_tag.values() for item in sublist]

        for fl in in_tables + in_files:
            self._upload_file(fl, sftp)

        sftp.close()
        conn.close()
        logging.info("Done.")

    @staticmethod
    def connect_to_server(port, host, user, password, pkey):
        try:
            conn = paramiko.Transport((host, port))
            conn.connect(username=user, password=password, pkey=pkey)
        except paramiko.ssh_exception.AuthenticationException:
            logging.error('Connection failed: recheck your authentication and host URL parameters')
            exit(1)
        except paramiko.ssh_exception.SSHException:
            logging.error('Connection failed: recheck your host URL and port parameters')
            exit(1)
        sftp = paramiko.SFTPClient.from_transport(conn)
        return sftp, conn

    def get_private_key(self, keystring):
        pkey = None
        if keystring:
            keyfile = StringIO(keystring)
            try:
                pkey = self._parse_private_key(keyfile)
            except (paramiko.SSHException, IndexError):
                logging.exception("Private Key is invalid")
                exit(1)
        return pkey

    @staticmethod
    def _parse_private_key(keyfile):
        # try all versions of encryption keys
        pkey = None
        failed = False
        try:
            pkey = paramiko.RSAKey.from_private_key(keyfile)
        except paramiko.SSHException:
            logging.warning("RSS Private key invalid, trying DSS.")
            failed = True
        # DSS
        if failed:
            try:
                pkey = paramiko.DSSKey.from_private_key(keyfile)
                failed = False
            except (paramiko.SSHException, IndexError):
                logging.warning("DSS Private key invalid, trying ECDSAKey.")
                failed = True
        # ECDSAKey
        if failed:
            try:
                pkey = paramiko.ECDSAKey.from_private_key(keyfile)
                failed = False
            except (paramiko.SSHException, IndexError):
                logging.warning("ECDSAKey Private key invalid, trying Ed25519Key.")
                failed = True
        # Ed25519Key
        if failed:
            try:
                pkey = paramiko.Ed25519Key.from_private_key(keyfile)
            except (paramiko.SSHException, IndexError) as e:
                logging.warning("Ed25519Key Private key invalid.")
                raise e

        return pkey

    def _upload_file(self, input_file, sftp):
        params = self.configuration.parameters

        destination = self.get_output_destination(input_file)
        logging.info(f"File Source: {input_file}")
        logging.info(f"File Destination: {destination}")
        try:
            self._try_to_execute_sftp_operation(sftp.put, input_file.full_path, destination)
        except FileNotFoundError:
            logging.exception(
                f"Destination path: '{params[KEY_REMOTE_PATH]}' in SFTP Server not found,"
                f" recheck the remote destination path")
            exit(1)
        except PermissionError:
            logging.exception(f"Permission Error: you do not have permissions to write to '{params[KEY_REMOTE_PATH]}',"
                              f" choose a different directory on the SFTP server")
            exit(1)

    def get_output_destination(self, input_file):
        params = self.configuration.parameters

        timestamp_suffix = ''
        if params[KEY_APPEND_DATE]:
            timestamp_suffix = "_" + str(datetime.utcnow().strftime('%Y%m%d%H%M%S'))

        file_path = params[KEY_REMOTE_PATH]
        if not file_path[-1] == "/":
            file_path = file_path + "/"

        filename, file_extension = os.path.splitext(os.path.basename(input_file.name))
        destination = file_path + filename + timestamp_suffix + file_extension
        return destination

    @backoff.on_exception(backoff.expo,
                          ConnectionError,
                          max_tries=MAX_RETRIES)
    def _try_to_execute_sftp_operation(self, operation: Callable, *args):
        return operation(*args)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(2)
