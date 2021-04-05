#!/usr/bin/env python3
"""
The script contains example of the paramiko usage for large file uploading.

It implements :func:`upload` with limited number of concurrent requests to server, whereas
paramiko implementation of the :method:`paramiko.SFTPClient.putfo` send read requests without
limitations, that can cause problems if large file is being downloaded.
"""
import ftplib
import os
import time
import socket


class UploadTracker:
    size_written = 0
    total_size = 0.0
    last_shown_percent = 0

    def __init__(self, total_size):
        self.total_size = total_size

    def handle(self, block):
        self.size_written += 1024
        percent_complete = round((self.size_written / self.total_size) * 100)

        if self.last_shown_percent != percent_complete:
            self.last_shown_percent = percent_complete
            print(str(percent_complete) + "% complete remaining: " + str(self.total_size - self.size_written))





if __name__ == "__main__":
    Server="servername.com"
    Username="username"
    Password="secret password"
    filename = "/path/to/folder"
    Directory="/path/on/server"

    tries = 0
    done = False


    print("Uploading " + str(filename) + " to " + str(Directory))

    while tries < 50 and not done:
        try:
            tries += 1
            with ftplib.FTP(Server) as ftp:
                ftp.set_debuglevel(2)
                print("login")

                ftp.login(Username, Password)
                ftp.cwd(Directory)

                with open(filename, 'rb') as f:
                    totalSize = os.path.getsize(filename)
                    print('Total file size : ' + str(round(totalSize / 1024 / 1024, 1)) + ' Mb')
                    uploadTracker = UploadTracker(int(totalSize))

                    # Get file size if exists
                    files_list = ftp.nlst()
                    if os.path.basename(filename) in files_list:
                        print("Resuming")
                        ftp.voidcmd('TYPE I')
                        rest_pos = ftp.size(os.path.basename(filename))
                        f.seek(rest_pos, 0)
                        print("seek to " + str(rest_pos))
                        uploadTracker.size_written = rest_pos
                        print(ftp.storbinary('STOR ' + os.path.basename(filename), f, blocksize=1024, callback=uploadTracker.handle, rest=rest_pos))
                    else:
                        print(ftp.storbinary('STOR ' + os.path.basename(filename), f, 1024, uploadTracker.handle))
                        done = True

        except (BrokenPipeError, ftplib.error_temp, socket.gaierror) as e:
            print(str(type(e)) + ": " + str(e))
            print("connection died, trying again")
            time.sleep(30)


    print("Done")