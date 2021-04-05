"""
    A file that contains constants of the applications
"""


class Constants:
    DEFAULT_DATE_FORMAT = 'YYYY-MM-DD_HH:mm:ss'
    DEFAULT_OUTPUT_DIR = '/'
    DEFAULT_DATA_DIR = '/data'
    CONFIG_FILE = 'config.json'
    SFTP_PROTOCOL = 'sftp'
    DEFAULT_SFTP_PORT = 22
    DEFAULT_RETRIES = 3
    DEFAULT_TIMEOUT = 300
    DEFAULT_PROTOCOL = SFTP_PROTOCOL
    DEFAULT_DATETIME_APPEND = False
    DEFAULT_CERTIFICATE_TRUST = True

    # Error messages
    ERROR_UNKNOWN_PROTOCOL = "Invalid protocol specified! Only SFTP or WebDAV are allowed"
