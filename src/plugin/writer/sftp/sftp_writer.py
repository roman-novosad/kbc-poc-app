"""
The script contains example of the paramiko usage for large file uploading.

It implements :func:`upload` with limited number of concurrent requests to server, whereas
paramiko implementation of the :method:`paramiko.SFTPClient.putfo` send read requests without
limitations, that can cause problems if large file is being downloaded.
"""





#!/usr/bin/env python3
import ftplib
import os
import time
import socket
from kbc.env_handler import KBCEnvHandler


class FtpUploadTracker:
    sizeWritten = 0
    totalSize = 0.0
    lastShownPercent = 0

    def __init__(self, totalSize):
        self.totalSize = totalSize

    def handle(self, block):
        self.sizeWritten += 1024
        percentComplete = round((self.sizeWritten / self.totalSize) * 100)

        if (self.lastShownPercent != percentComplete):
            self.lastShownPercent = percentComplete
            print(str(percentComplete) + "% complete remaining: " + str(self.totalSize - self.sizeWritten), flush=True)



if __name__ == "__main__":
    Server="servername.com"
    Username="username"
    Password="secret password"
    filename = "/path/to/folder"
    Directory="/path/on/server"

    tries = 0
    done = False

    print("Uploading " + str(filename) + " to " + str(Directory), flush=True)

    while tries < 50 and not done:
        try:
            tries += 1
            with ftplib.FTP(Server) as ftp:
                ftp.set_debuglevel(2)
                print("login", flush=True)
                ftp.login(Username, Password)
                # ftp.set_pasv(False)
                ftp.cwd(Directory)
                with open(filename, 'rb') as f:
                    totalSize = os.path.getsize(filename)
                    print('Total file size : ' + str(round(totalSize / 1024 / 1024 ,1)) + ' Mb', flush=True)
                    uploadTracker = FtpUploadTracker(int(totalSize))

                    # Get file size if exists
                    files_list = ftp.nlst()
                    print(files_list, flush=True)
                    if os.path.basename(filename) in files_list:
                        print("Resuming", flush=True)
                        ftp.voidcmd('TYPE I')
                        rest_pos = ftp.size(os.path.basename(filename))
                        f.seek(rest_pos, 0)
                        print("seek to " + str(rest_pos))
                        uploadTracker.sizeWritten = rest_pos
                        print(ftp.storbinary('STOR ' + os.path.basename(filename), f, blocksize=1024, callback=uploadTracker.handle, rest=rest_pos), flush=True)
                    else:
                        print(ftp.storbinary('STOR ' + os.path.basename(filename), f, 1024, uploadTracker.handle), flush=True)
                        done = True

        except (BrokenPipeError, ftplib.error_temp, socket.gaierror) as e:
            print(str(type(e)) + ": " + str(e))
            print("connection died, trying again")
            time.sleep(30)


    print("Done")