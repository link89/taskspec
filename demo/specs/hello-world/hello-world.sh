#!/bin/bash
#SBATCH -N 1 -n 1 -c 1
#SBATCH -p cpu

echo "Hello, World!" > output.txt