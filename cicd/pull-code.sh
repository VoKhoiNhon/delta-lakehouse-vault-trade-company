#!/bin/bash

################
# Init Variables
################
abs_filepath=$(readlink -f $0)
abs_dir=$(dirname $abs_filepath)
project_dir=$(dirname $abs_dir)
echo "--LOG: Project Directory: $project_dir"

#########
# Execute
#########
reset_to_main_branch() {
    cd $project_dir
    sudo git fetch --all
    sudo git switch main
    sudo git reset --hard origin/main
}
main() {
    reset_to_main_branch
}
main
