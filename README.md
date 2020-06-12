# Resilient cp

This python script allows you to copy a large number of files to a mounted network drive using cp, and easily recover in the case of failures (e.g. network disconnections).

Example usage:

``` python resilient_cp.py -s ~/Documents/large_folder/ -t ~/mounted_drive/target_folder/ ```


When the script is first run, it will create a JSON file to keep track of which files have been transferred. If the script dies half way through, this file can be used with the `-j` command line argument to allow the script to start again, only focussing on files which still need to be transferred.

In the case of a bad network connection, the script will sleep for 15 minutes before reattempting to transfer the file up to max_retries times.

## TODO:

* Update repository name
* Improve network connection checking

