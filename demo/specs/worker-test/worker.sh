#!/bin/bash
#SBATCH -N 1 -n 1 -c 1
#SBATCH -p cpu
#SBATCH -t 00:02:00
source /public/scripts/proxy.sh
python3 -u worker.py
