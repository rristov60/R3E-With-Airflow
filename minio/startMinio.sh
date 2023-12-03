#!/bin/bash

MINIO_ROOT_USER=admin MINIO_ROOT_PASSWORD=password ./minio server . --console-address ":9001"