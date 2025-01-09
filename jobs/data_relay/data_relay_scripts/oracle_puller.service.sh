#!/usr/bin/bash

# This script is launched by the systemd service described in the `oracle_puller.service` file.
# - The `source /home/jupyter/.bashrc` command loads the environment, including database credentials,
#   into the shell. This allows the Python code in `oracle_puller_daemon.py` to authenticate with 
#   the Oracle database.
# - The script then navigates to the working directory (`/data/projects/crawler`) and executes 
#   the `oracle_puller_daemon.py` script.
#
# Callout:
# - The `oracle_puller_daemon.py` script (and its associated `oracle_puller.service`) can only 
#   be run from the `svgcmdl01.dot.ca.gov` machine under the `jupyter` username. 
# - This restriction exists because the Oracle Database access is limited to this machine and user 
#   for security reasons.
#
# Note:
# - The script is designed to be short-lived, with a total runtime of less than 1 hour, 
#   as controlled by the loop's exit condition in `oracle_puller_daemon.py`.
# - Continuous data pulling is maintained through the systemd service's `Restart=always` configuration, 
#   which ensures the script restarts automatically after it exits.
# - The script also relies on the recurring controlling scripts (`append_to_crawl_queue*.py`) to 
#   provide continuous instructions and queue new tasks for processing.
#

source /home/jupyter/.bashrc

cd /data/projects/crawler

python3.9 oracle_puller_daemon.py


