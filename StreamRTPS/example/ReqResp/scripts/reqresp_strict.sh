#!/usr/bin/env bash
# Written in [Amber](https://amber-lang.com/)
# version: 0.5.1-alpha

file_exists__strict() {
    local path=$1
    [ -f "${path}" ]
    __status=$?
    ret_file_exists_strict="$(( ${__status} == 0 ))"
    return 0
}

declare -r args_3=("$0" "$@")
echo "Executing Strict ReqResp Verification!"
command_1="$(echo $PWD)"
__status=$?
if [ "${__status}" != 0 ]; then
    echo "Failed to get current working dir."
    exit 1
fi
current_dir_4="${command_1}"
build_dir_5="${current_dir_4}""/build/"

strict_sender_6="${build_dir_5}""StrictSender"
strict_responder_7="${build_dir_5}""StrictResponder"
sender_log_8="${build_dir_5}""StrictSender.txt"
responder_log_9="${build_dir_5}""StrictResponder.txt"

file_exists__strict "${strict_sender_6}"
ret_file_exists_strict__12_12="${ret_file_exists_strict}"
if [ "$(( ! ${ret_file_exists_strict__12_12} ))" != 0 ]; then
    echo "StrictSender missing, looking in ""${build_dir_5}"" for StrictSender"
    exit 1
fi

file_exists__strict "${strict_responder_7}"
ret_file_exists_strict__17_12="${ret_file_exists_strict}"
if [ "$(( ! ${ret_file_exists_strict__17_12} ))" != 0 ]; then
    echo "StrictResponder missing, looking in ""${build_dir_5}"" for StrictResponder"
    exit 1
fi

mkdir -p "${build_dir_5}"
__status=$?
if [ "${__status}" != 0 ]; then
    exit "${__status}"
fi

rm -f "${sender_log_8}" "${responder_log_9}"
__status=$?
if [ "${__status}" != 0 ]; then
    exit "${__status}"
fi

"${strict_sender_6}" > "${sender_log_8}" 2>&1 &
__status=$?
if [ "${__status}" != 0 ]; then
    echo "Failed to start StrictSender."
    exit 1
fi
sender_pid_10="$(echo $!)"

"${strict_responder_7}" > "${responder_log_9}" 2>&1 &
__status=$?
if [ "${__status}" != 0 ]; then
    echo "Failed to start StrictResponder."
    kill "${sender_pid_10}" >/dev/null 2>&1
    exit 1
fi
responder_pid_11="$(echo $!)"

timeout 70s bash -c "while kill -0 ${sender_pid_10} 2>/dev/null || kill -0 ${responder_pid_11} 2>/dev/null; do sleep 1; done"
__status=$?
if [ "${__status}" != 0 ]; then
    echo "Strict ReqResp verification timed out."
    kill "${sender_pid_10}" "${responder_pid_11}" >/dev/null 2>&1
    sleep 1
    kill -9 "${sender_pid_10}" "${responder_pid_11}" >/dev/null 2>&1
    exit 1
fi

wait "${sender_pid_10}"
__status=$?
if [ "${__status}" != 0 ]; then
    echo "StrictSender exited with failure."
    exit 1
fi

wait "${responder_pid_11}"
__status=$?
if [ "${__status}" != 0 ]; then
    echo "StrictResponder exited with failure."
    exit 1
fi

grep -q "TEST_PASS" "${sender_log_8}"
__status=$?
if [ "${__status}" != 0 ]; then
    echo "StrictSender log is missing TEST_PASS marker."
    exit 1
fi

grep -q "TEST_PASS" "${responder_log_9}"
__status=$?
if [ "${__status}" != 0 ]; then
    echo "StrictResponder log is missing TEST_PASS marker."
    exit 1
fi

echo "Strict ReqResp verification PASSED"
exit 0
