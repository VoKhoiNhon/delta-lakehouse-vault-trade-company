#!/bin/bash

# Parsing absolute filepath of this script
abs_filepath=$(readlink -f "$0")
abs_dirpath=$(dirname "$abs_filepath")
build_dirpath=$(dirname "$abs_dirpath")

#################
# Build Image
#################
build_image() {
    DOCKER_BUILDKIT=1 docker build -t pyjob:latest -f "$build_dirpath"/Dockerfile --target pyjob "$build_dirpath"
    DOCKER_BUILDKIT=1 docker build -t jupyter-lab:latest -f "$build_dirpath"/Dockerfile --target jupyter-lab "$build_dirpath"
    DOCKER_BUILDKIT=1 docker build -t bitnami/spark:3.5.1-custom -f "$build_dirpath"/Dockerfile --target bitnami-spark-custom "$build_dirpath"
    DOCKER_BUILDKIT=1 docker build -t hive-metastore:latest -f "$build_dirpath"/Dockerfile --target hive-metastore "$build_dirpath"
    DOCKER_BUILDKIT=1 docker build -t apache/superset:4.1.1-custom -f "$build_dirpath"/Dockerfile --target superset-custom "$build_dirpath"
}
##################
# Clean None Image
##################
clean_image() {
docker images | awk '$1 == "<none>" && $2 == "<none>" {print $3}' | xargs -I {} docker image rm {}
}

main() {
    build_image
    clean_image
}
main
