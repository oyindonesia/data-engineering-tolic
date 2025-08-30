#!/bin/bash

# Variables
REMOTE_USER="oygiovannorachmat"
REMOTE_HOST="10.130.4.80"
REMOTE_DIR="../data_engineer/bi-projects/"
VENV_DIR="venv_data_eng"

# SSH into the remote host and run the commands
ssh -t "${REMOTE_USER}@${REMOTE_HOST}" << EOF
  cd "${REMOTE_DIR}"
  sudo su data_engineer
  source "${VENV_DIR}/bin/activate"
  nohup jupyter notebook --ip 0.0.0.0 --port 8888 &
  exit
EOF

echo "Jupyter Notebook server started on remote host ${REMOTE_HOST}"
