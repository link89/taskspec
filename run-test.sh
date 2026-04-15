#!/bin/bash

# Unset proxy environment variables to prevent interference with local tests
unset http_proxy
unset https_proxy
unset ftp_proxy
unset all_proxy
unset no_proxy

unset HTTP_PROXY
unset HTTPS_PROXY
unset FTP_PROXY
unset ALL_PROXY
unset NO_PROXY

echo "Proxy environment variables unset. Running tests..."

# Run pytest on the e2e tests
pytest -s tests/test_e2e.py
