#!/usr/bin/env bash

DEFAULT_INFLUXDB3_HOME=/usr/local/influxb3
PROFILE="${INFLUXDB3_PROFILE:-quick-release}"

INFLUXDB3_PRJ_HOME="${INFLUXDB3_PRJ_HOME:-${DEFAULT_INFLUXDB3_HOME}}"
INFLUXDB3_EXE="${INFLUXDB3_PRJ_HOME}/target/${PROFILE}/influxdb3"

SECURE_DEPLOY="${INFLUXDB3_SECURE_DEPLOY:-TRUE}"
OBJECT_STORE="${INFLUXDB3_OBJECT_STORE:-file}"
HOST_ID="${INFLUXDB3_HOST_ID:-client_test}"
DATA_DIR="${INFLUXDB3_DATA_DIR:-/tmp/influxdb3/data}"
PROCESS_FILE="$(pwd)/influxdb3.pid"

if [ -f "${PROCESS_FILE}" ]
then
  OLD_PROCESS=$(cat ${PROCESS_FILE})
  if [ -d "/proc/${OLD_PROCESS}" ]
  then
    printf "Influxdb3 already running as process %s\n" "${OLD_PROCESS}"
    printf "EXITING\n"
    exit 0
  fi
fi

if ! [ -d "${INFLUXDB3_PRJ_HOME}" ]
then
  printf "Directory for INFLUXDB3_PRJ_HOME %s not found.\n" "${INFLUXDB3_PRJ_HOME}"
  printf "Set the environment variable INFLUXDB3_PRJ_HOME to the local project location.\n"
  printf "ABORTING\n"
  exit 1
fi

if ! [ -f "${INFLUXDB3_PRJ_HOME}/Cargo.toml" ]
then
  printf "%s exists but is not a Rust/Cargo project.\n" "${INFLUXDB3_PRJ_HOME}"
  printf "ABORTING\n"
  exit 1
fi

if [ -x "${INFLUXDB3_EXE}" ]
then
  printf "found %s\n" "$(ls "${INFLUXDB3_EXE}")"
  printf "build skipped\n"
  printf "\nto force rebuild run '$ cargo clean' in directory %s, then run this script again\n\n" "${INFLUXDB3_PRJ_HOME}"
else
  command -v cargo >/dev/null 2>&1 || { printf "Build requires cargo, but it could not be found.\nABORTING.\n"; exit 1; }
  START_DIR=$(pwd)
  echo DEBUG START_DIR "${START_DIR}"
  cd "${INFLUXDB3_PRJ_HOME}" || exit
  printf "Building Influxdb3 with profile %s\n" "${PROFILE}"
  printf "Building in %s" "$(pwd)\n"
  printf "This may take a few minutes\n"
  cargo build --package="influxdb3" --profile="${PROFILE}" --no-default-features --features="jemalloc_replacing_malloc"
  BUILD_RESULT=$?
  # shellcheck disable=SC2164
  cd "${START_DIR}" || cd -
  printf "Build end status %s\n" "${BUILD_RESULT}"
  if [ "${BUILD_RESULT}" != 0 ]
  then
    printf "Build failed\n"
    printf "ABORTING\n"
    exit ${BUILD_RESULT}
  fi
fi

if [ ! -e "${INFLUXDB3_EXE}" ]
then
  printf "Failed to locate influxdb3 executable %s\n" "${INFLUXDB3_EXE}"
  printf "ABORTING\n"
  exit 1
fi

if [ ! -x "${INFLUXDB3_EXE}" ]
then
  printf "Found influxdb3 file %s\n" "${INFLUXDB3_EXE}"
  printf "But it is not executable\n"
  printf "ABORTING"
  exit 1
fi

printf "Preparing to deploy\n"
DEPLOY_ARGS="--object-store ${OBJECT_STORE} --data-dir ${DATA_DIR} --log-filter DEBUG"

if [ "${SECURE_DEPLOY}" == "TRUE" ]
then
  TOKEN_RESULT=$(${INFLUXDB3_EXE} create token | head -n 2 | sed ':a;N;$!ba;s/\n/#/g')
  TOKEN="$(echo "$TOKEN_RESULT" | sed s/\#.*$//g | sed s/^Token:\ //)"
  HASHED_TOKEN="$(echo "$TOKEN_RESULT" | sed s/^.*\#//g | sed s/Hashed\ Token:\ //)"
  DEPLOY_ARGS="${DEPLOY_ARGS} --bearer-token ${HASHED_TOKEN}"
  printf "User Token will be:\n%s\nStore this somewhere and use it when calling the Influx server\n" "${TOKEN}"
  echo "export INFLUXDB_TOKEN=${TOKEN}" > influxdb3.token
fi

if [ ! -d "${DATA_DIR}" ]
then
  printf "Making data dir %s\n" "${DATA_DIR}"
  mkdir -p ${DATA_DIR}
  RESULT=$?
  if [ "${RESULT}" != 0 ] && [ "${OBJECT_STORE}" == "file" ]
  then
    printf "Failed to create %s when using `file` object store\n" "${DATA_DIR}"
    printf "ABORTING"
    exit ${RESULT}
  fi
fi

if [ ! -w "${DATA_DIR}" ] && [ "${OBJECT_STORE}" == "file" ]
then
  printf "Data dir %s is not writable when using `file` object store\n" "${DATA_DIR}"
  print "ABORTING"
  exit 1
fi

#${INFLUXDB3_EXE} serve --host-id kk-local --object-store file --data-dir /home/karl/temp/store/db --log-filter DEBUG > influxdb3.log 2>&1 &
${INFLUXDB3_EXE} serve ${DEPLOY_ARGS} --writer-id TEST > influxdb3.log 2>&1 &
echo $! | tee influxdb3.pid
