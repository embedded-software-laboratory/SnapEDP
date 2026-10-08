#!/usr/bin/env bash
# Written in [Amber](https://amber-lang.com/)
# version: 0.5.1-alpha
# We cannot import `bash_version` from `env.ab` because it imports `text.ab` making a circular dependency.
# This is a workaround to avoid that issue and the import system should be improved in the future.
file_exists__37_v0() {
    local path=$1
    [ -f "${path}" ]
    __status=$?
    ret_file_exists37_v0="$(( ${__status} == 0 ))"
    return 0
}

declare -r args_3=("$0" "$@")
echo "Executing Request Response Example"'!'""
command_1="$(echo $PWD)"
__status=$?
if [ "${__status}" != 0 ]; then
    echo "Failed to get current working dir."
fi
current_dir_4="${command_1}"
build_dir_5="${current_dir_4}""/build/"
file_exists__37_v0 "${build_dir_5}""Sender"
ret_file_exists37_v0__12_12="${ret_file_exists37_v0}"
if [ "$(( ! ${ret_file_exists37_v0__12_12} ))" != 0 ]; then
    echo "Sender missing, looking in ""${build_dir_5}"" for Sender"
    exit 1
fi
file_exists__37_v0 "${build_dir_5}""Responder"
ret_file_exists37_v0__17_12="${ret_file_exists37_v0}"
if [ "$(( ! ${ret_file_exists37_v0__17_12} ))" != 0 ]; then
    echo "Responder missing, looking in ""${build_dir_5}"" for Responder"
    exit 1
fi
mkdir -p ${build_dir_5}
__status=$?
pcap_tmp_6="/tmp/streamrtps_reqresp_record.pcap"
pcap_path_7="${build_dir_5}""record.pcap"
__sudo=$([ "$(id -u)" -ne 0 ] && command -v sudo >/dev/null 2>&1 && printf sudo)
${__sudo} rm -f ${pcap_tmp_6}
__status=$?
if [ "${__status}" != 0 ]; then
    exit "${__status}"
fi
__sudo=$([ "$(id -u)" -ne 0 ] && command -v sudo >/dev/null 2>&1 && printf sudo)
${__sudo} tshark -i eno1 -w ${pcap_tmp_6} -q >/dev/null &
__status=$?
if [ "${__status}" != 0 ]; then
    echo "Starting tshark failed."
fi
"${build_dir_5}""Sender" > "${build_dir_5}""Sender".txt &
__status=$?
command_2="$(echo $!)"
__status=$?
if [ "${__status}" != 0 ]; then
    exit "${__status}"
fi
sender_pid_8="${command_2}"
"${build_dir_5}""Responder" > "${build_dir_5}""Responder".txt &
__status=$?
command_3="$(echo $!)"
__status=$?
if [ "${__status}" != 0 ]; then
    exit "${__status}"
fi
responder_pid_9="${command_3}"
# Give them more time to sleep
sleep 15
__status=$?
kill ${sender_pid_8} >/dev/null 2>&1
__status=$?
if [ "${__status}" != 0 ]; then
    exit "${__status}"
fi
kill ${responder_pid_9} >/dev/null 2>&1
__status=$?
if [ "${__status}" != 0 ]; then
    exit "${__status}"
fi
__sudo=$([ "$(id -u)" -ne 0 ] && command -v sudo >/dev/null 2>&1 && printf sudo)
${__sudo} pkill -f "tshark -i lo -w ${pcap_tmp_6}"
__status=$?
if [ "${__status}" != 0 ]; then
    exit "${__status}"
fi
__sudo=$([ "$(id -u)" -ne 0 ] && command -v sudo >/dev/null 2>&1 && printf sudo)
${__sudo} chmod a+r ${pcap_tmp_6}
__status=$?
if [ "${__status}" != 0 ]; then
    exit "${__status}"
fi
cp ${pcap_tmp_6} ${pcap_path_7}
__status=$?
if [ "${__status}" != 0 ]; then
    exit "${__status}"
fi
echo "Terminated experiment."
exit 0
