#!/bin/bash

# Define color codes
RED=$(tput setaf 1)
BLUE=$(tput setaf 4)
NORMAL=$(tput sgr0)

# Default Variables
abs_filepath=$(readlink -f $0)
abs_dirpath=$(dirname $abs_filepath)
project_dirpath=$(dirname $abs_dirpath)
echo "${BLUE}LOG: Project Working Dir: $project_dirpath ${NORMAL}"
max_memory_gb=$(free -g | awk '/^Mem:/{print $2}')
used_memory_gb="$(( (max_memory_gb / 3) * 2 ))GB"
echo "${BLUE}LOG: Maximum Memory: $max_memory_gb GB ${NORMAL}"
echo "${BLUE}LOG: Used Memory: $used_memory_gb ${NORMAL}"
max_cores=$(nproc)
used_cores=$((max_cores/2))
echo "${BLUE}LOG: Maximum Cores: $max_cores ${NORMAL}"
echo "${BLUE}LOG: Used Cores: $used_cores ${NORMAL}"

#####################################
# Generate Template Environment Files
#####################################
gen_temp_env_file() {
cat <<EOF > "$project_dirpath/.env"
# This is the project of ProcessingData with SparkCluster which contains file config.
# Default: "\$project_dirpath"
PROJECT_DIR=$project_dirpath
SPARK_WORKER_MEMORY=$used_memory_gb
SPARK_WORKER_CORES=$used_cores
EOF

    echo "${BLUE}LOG: Preview file $project_dirpath/.env ${NORMAL}"
    cat $project_dirpath/.env
}

# Execute: bash scripts/gen_template_env_files.sh
gen_temp_env_file
