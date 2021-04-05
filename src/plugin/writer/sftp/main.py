"""
The script contains example of the paramiko usage for large file downloading.

It implements :func:`download` with limited number of concurrent requests to server, whereas
paramiko implementation of the :meth:`paramiko.SFTPClient.getfo` send read requests without
limitations, that can cause problems if large file is being downloaded.
"""

import logging
import os
import typing
from os.path import join, dirname

from paramiko import SFTPClient, SFTPFile, Message, SFTPError, Transport
from paramiko.sftp import CMD_STATUS, CMD_READ, CMD_DATA

logger = logging.getLogger('demo')


class _SFTPFileDownloader:
    """
    Helper class to upload large file with paramiko sftp client with limited number of concurrent requests.
    """

    _DOWNLOAD_MAX_REQUESTS = 48
    _DOWNLOAD_MAX_CHUNK_SIZE = 0x8000

    def __init__(self, f_in: SFTPFile, f_out: typing.BinaryIO, callback=None):
        self.f_in = f_in
        self.f_out = f_out
        self.callback = callback

        self.requested_chunks = {}
        self.received_chunks = {}
        self.saved_exception = None

    def download(self):
        file_size = self.f_in.stat().st_size
        requested_size = 0
        received_size = 0

        while True:
            # send read requests
            while len(self.requested_chunks) + len(self.received_chunks) < self._DOWNLOAD_MAX_REQUESTS and \
                    requested_size < file_size:
                chunk_size = min(self._DOWNLOAD_MAX_CHUNK_SIZE, file_size - requested_size)
                request_id = self._sftp_async_read_request(
                    fileobj=self,
                    file_handle=self.f_in.handle,
                    offset=requested_size,
                    size=chunk_size
                )
                self.requested_chunks[request_id] = (requested_size, chunk_size)
                requested_size += chunk_size

            # receive blocks if they are available
            # note: the _async_response is invoked
            self.f_in.sftp._read_response()
            self._check_exception()

            # write received data to output stream
            while True:
                chunk = self.received_chunks.pop(received_size, None)
                if chunk is None:
                    break
                _, chunk_size, chunk_data = chunk
                self.f_out.write(chunk_data)
                if self.callback is not None:
                    self.callback(chunk_data)

                received_size += chunk_size

            # check transfer status
            if received_size >= file_size:
                break

            # check chunks queues
            if not self.requested_chunks and len(self.received_chunks) >= self._DOWNLOAD_MAX_REQUESTS:
                raise ValueError("SFTP communication error. The queue with requested file chunks is empty and"
                                 "the received chunks queue is full and cannot be consumed.")

        return received_size

    def _sftp_async_read_request(self, fileobj, file_handle, offset, size):
        sftp_client = self.f_in.sftp

        with sftp_client._lock:
            num = sftp_client.request_number

            msg = Message()
            msg.add_int(num)
            msg.add_string(file_handle)
            msg.add_int64(offset)
            msg.add_int(size)

            sftp_client._expecting[num] = fileobj
            sftp_client.request_number += 1

        sftp_client._send_packet(CMD_READ, msg)
        return num

    def _async_response(self, t, msg, num):
        if t == CMD_STATUS:
            # save exception and re-raise it on next file operation
            try:
                self.f_in.sftp._convert_status(msg)
            except Exception as e:
                self.saved_exception = e
            return
        if t != CMD_DATA:
            raise SFTPError("Expected data")
        data = msg.get_string()

        chunk_data = self.requested_chunks.pop(num, None)
        if chunk_data is None:
            return

        # save chunk
        offset, size = chunk_data

        if size != len(data):
            raise SFTPError(f"Invalid data block size. Expected {size} bytes, but it has {len(data)} size")
        self.received_chunks[offset] = (offset, size, data)

    def _check_exception(self):
        """if there's a saved exception, raise & clear it"""
        if self.saved_exception is not None:
            x = self.saved_exception
            self.saved_exception = None
            raise x


class _SFTPFileUploader:

    def upload_directory(self, remote_path: str, local_path: str):
        """Uploads the contents of the source directory to the target path. The
            target directory needs to exists. All subdirectories in source are
            created under target.
        """
        for item in os.listdir(local_path):
            if os.path.isfile(os.path.join(local_path, item)):
                self.put(os.path.join(local_path, item), '%s/%s' % (remote_path, item))
            else:
                self.mkdir('%s/%s' % (remote_path, item), ignore_existing=True)
                self.put_dir(os.path.join(local_path, item), '%s/%s' % (remote_path, item))

    def mkdir(self, path, mode=511, ignore_existing=False):
        """ Augments mkdir by adding an option to not fail if the folder exists """
        try:
            super(_SFTPFileUploader, self) \
                .mkdir(path, mode)
        except IOError:
            if ignore_existing:
                pass
            else:
                raise


def download_file(sftp_client: SFTPClient, remote_path: str, local_path: str, callback=None):
    """
    Helper function to download remote file via sftp.

    It contains a fix for a bug that prevents a large file downloading with :meth:`paramiko.SFTPClient.get`

    Note: this function relies on some private paramiko API and has been tested with paramiko 2.7.1.
          So it may not work with other paramiko versions.

    :param sftp_client: paramiko sftp client
    :param remote_path: remote file path
    :param local_path: local file path
    :param callback: optional data callback
    """
    remote_file_size = sftp_client.stat(remote_path).st_size

    with sftp_client.open(remote_path, 'rb') as f_in, open(local_path, 'wb') as f_out:
        _SFTPFileDownloader(
            f_in=f_in,
            f_out=f_out,
            callback=callback
        ).download()

    local_file_size = os.path.getsize(local_path)
    if remote_file_size != local_file_size:
        raise IOError(f"file size mismatch: {remote_file_size} != {local_file_size}")


def upload_file(sftp_client: SFTPClient, remote_path: str, local_path: str, callback=None):
    """
    Helper function to upload local file or directory to remote via sftp.

    :param sftp_client: paramiko sftp client
    :param remote_path: remote file path
    :param local_path: local file path
    :param callback: optional data callback
    """

    split_path = remote_path.split('/')
    if len(split_path) > 1:

        remote_directory = '/'.join(split_path[0:len(split_path) - 1])
        sftp_client.mkdir(remote_directory)
        sftp_client.chdir(remote_directory)
        sftp_client.put(local_path, '/'.join(split_path[len(split_path) - 1:]))
    else:
        sftp_client.put(local_path, remote_path)  # At this point, you are in remote_path in either case


def main():
    # public sftp server
    host = 'demo.wftpserver.com'
    port = 2222
    username = 'demo-user'
    password = 'demo-user'
    remote_file_path = '/download/manual_en.pdf'
    local_file_path = join(dirname(__file__), 'manual_en.pdf')

    transport = Transport((host, port))
    transport.set_keepalive(30)
    transport.connect(
        username=username,
        password=password,
    )
    with SFTPClient.from_transport(transport) as sftp_client:
        progress_size = 0
        total_size = 0
        step_size = 4 * 1024 * 1024

        def progress_callback(data):
            nonlocal progress_size, total_size
            progress_size += len(data)
            total_size += len(data)

            while progress_size >= step_size:
                logger.info(f"{total_size // (1024 * 1024)} MB has been downloaded")
                progress_size -= step_size

        download_file(
            sftp_client=sftp_client,
            remote_path=remote_file_path,
            local_path=local_file_path,
            callback=progress_callback
        )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")
    main()
