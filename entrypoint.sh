#!/bin/bash
export MYSQL_ROOT_PASSWORD=$(cat /run/secrets/MYSQL_ROOT_PASSWORD)
uvicorn main:api --host 0.0.0.0
