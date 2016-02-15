#!/bin/bash

###########################################################################
# Configuration
###########################################################################

# This uid will be passed into configuration. All requests with this uid
# will be proxied from master to replica (but not from replica to master)
test_uid=1000060304  # pavel@mail.ru (dev user)

#test_uid_for_remote_shard=602520557 # p.berezhnoy@corp.mail.ru
#test_uid_for_local_shard=602520557 # p.berezhnoy@corp.mail.ru

master_host='127.0.0.1'
master_port=30071
master_admin_port=32071

replica_host='127.0.0.1'
replica_port=30072
replica_admin_port=32072

# Indexes, used to understand where we should store current tuple
# XXX: for configuration
#  +-------+--------------------+
#  | db_id | db_addr            |
#  +-------+--------------------+
#  |  1024 | 11.80.232.50:30071 |
#  +-------+--------------------+
#  first_index == 1
#  last_index == 1024

master_first_index=1
master_last_index=512

replica_first_index=513
replica_last_index=1024

# Remove this option in production
TEST_MODE=1

LSN_number_dump_file=/tmp/LSN.dump
LSN_request_timeout=0.1

# Timeout for box.net.box (in seconds, float numbers are supported)
netbox_timeout=1

_echo() {
    >&2 echo "$(date +"%T.%5N") >>> $*"
}

PID=$$
_EXIT() {
    # We should use suicide because a=$(exit 1) returns without real exit
    kill -KILL $PID
}

restore_tarantool() {
    # This function should be called before any exit
    # It should disable resharding on master and on slave and (possibly) revert MySQL table content.

    echo 'lua addrbook_disable_resharding()' | nc $master_host $master_admin_port
    echo 'lua addrbook_disable_resharding()' | nc $replica_host $replica_admin_port
}

update_mysql_configuration() {
    # XXX: TODO: Implement this function !!!!!
    # This function should update tarantools configuration into MySQL

    if [ $TEST_MODE -eq 1 ]; then
        # XXX: Remove me
        echo "insert into mPOP2.addrbook_dbs(db_id, db_addr, db_rep_addr) values ($((replica_first_index - 1)), '$replica_host:$replica_port', '$replica_host:$replica_port')" | mysql -umpop
    else
        restore_tarantool
        _EXIT
    fi
}

replica_disable_replication() {
    # XXX: TODO: Implement this function !!!!!
    # This function should update replica's config (it should disable replication on replica)
    # and it should reload configuration.
    # To call tarantool via nc, use send_req (or send_req_no_error) function.

    if [ $TEST_MODE -eq 1 ]; then
        # XXX: Remove me
        perl -pi -e 's/^replication_source/#replication_source/' /etc/tarantool/tarantool_box7.2.cfg
        send_req $replica "reload configuration"
    else
        restore_tarantool
        _EXIT
    fi
}

master="master"
replica="replica"
send_req() {
    local t=$1 # master or replica

    local cmd="${@:2}"

    local host=$master_host
    local port=$master_admin_port

    if [ $t == $replica ]; then
        host=$replica_host
        port=$replica_admin_port
    elif [ $t != $master ]; then
        _echo "ACHTUNG! Invalid tarantool type: '$t' !!! Disable resharding and Die"

        restore_tarantool

        _EXIT
    fi

    _echo "Trying to execute [ $cmd ] on $t ($host:$port)"
    echo $cmd | nc $host $port | tr -d '\n'
}

continue_or_die() {
    local msg=$1

    read -p "$msg (y/N) " -n 1 -r
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        # XXX: Should we restore tarantool here ?
        # restore_tarantool
        _EXIT
    fi
}

send_req_no_error() {
    local ret=$(send_req $*)

    _echo $ret

    echo $ret | egrep '\b(error:|unknown command.)'
    if [ $? == 0 ]; then
        _echo "ACHTUNG! Can't execute query: '$2' on $1! Disable resharding and Die"
        _echo "Ret:"
        _echo "$ret"

        restore_tarantool
        _EXIT
    fi

    echo $ret
}

send_req_no_error_both() {
    send_req_no_error $master $1
    send_req_no_error $replica $1
}

is_ready() {
    local host=$1
    local port=$2

    local ret=$(echo 'lua addrbook_disable_resharding' | nc $host $port)
    if [ $? != 0 ]; then
        _echo "Can't access $host:$port"
        _EXIT
    fi

    echo $ret | fgrep 'function'
    if [ $? != 0 ]; then
        _echo "call 'lua dofile(\"addrbook_resharding.lua\")' on $host:$port"
        _EXIT
    fi
}

restore_configuration_impl() {
    local t=$1
    local args=$2
    local extra_conf="{}"
    if [ $3 != "" ]; then
        extra_conf=$3
    fi

    send_req $t 'lua addrbook_disable_resharding()'

    _echo
    send_req_no_error $t "lua resharding.set_configuration($args, $extra_conf)"
}

mk_extra_conf_test() {
    local extra_conf="{test_key=$test_uid,"
    if [ "$netbox_timeout" != "" ]; then
        extra_conf+="netbox_timeout=$netbox_timeout,"
    fi
    extra_conf+="}"

    echo $extra_conf
}

mk_extra_conf_common() {
    local extra_conf="{"
    if [ "$netbox_timeout" != "" ]; then
        extra_conf+="netbox_timeout=$netbox_timeout,"
    fi
    extra_conf+="}"

    echo $extra_conf
}

restore_configuration() {
    local extra_conf=$1

    restore_configuration_impl $master "$master_first_index, $master_last_index, '$replica_host', $replica_port" $extra_conf
    restore_configuration_impl $replica "$replica_first_index, $replica_last_index, '$master_host', $master_port" $extra_conf
}

check_content_same() {
    local test_name=$1
    local t=$2
    local space=$3
    local old_data="${@:4}"

    local ret=$(send_req $t "lua box.select($space, 0, $test_uid)")

    if [[ $ret != $old_data ]]; then
        _echo "ERROR! Dryrun test '$test_name' failed! (type == $t, ret != old_data, space == $space) Restore shards configuration and Die!"
        _echo "EXPECTED: $old_data"
        _echo "FOUND: $ret"

        restore_tarantool

        _EXIT
    fi
}

check_modification() {
    echo "$@" | fgrep "error: 'Can''t modify data on a replication slave"
    if [ $? -ne 0 ]; then
        _echo "ERROR! Modification occured on slave!!! Restore shards configuration and Die!"

        restore_tarantool

        _EXIT
    fi
}

test_dryrun_mode_impl() {
    _echo "Requesting existing data from both tarantools..."

    local master_space_0_content=$(send_req $master "lua box.select(0, 0, $test_uid)")
    local master_space_1_content=$(send_req $master "lua box.select(1, 0, $test_uid)")

    local replica_space_0_content=$(send_req $replica "lua box.select(0, 0, $test_uid)")
    local replica_space_1_content=$(send_req $replica "lua box.select(1, 0, $test_uid)")

    local uid="box.pack('i', $test_uid)"
    local date="box.pack('i', $(date +%s))"
    local email="'test_e@mail.ru'"
    local name="'just a test'"

    # request should be passed from master to replica. Master shouldn't contain changed data and replica should
    # revert modify requests (just because it is still a replica).
    local ret=$(send_req $master "lua addrbook_add_recipient($uid, $email, $name, $date)")
    check_modification $ret
    check_content_same "addrbook_add_recipient_master" $master 1 $master_space_1_content
    check_content_same "addrbook_add_recipient_replica" $replica 1 $replica_space_1_content

    #####

    send_req_no_error $master "lua addrbook_get_recipients($uid)"

    #####

    send_req_no_error $master "lua addrbook_get($uid)"

    ####

    ret=$(send_req $master "lua addrbook_put($uid, $name)") # replace with another data
    check_modification $ret
    check_content_same "addrbook_put_master" $master 0 $master_space_0_content
    check_content_same "addrbook_put_replica" $replica 0 $replica_space_0_content

    ####

    ret=$(send_req $master "lua addrbook_delete($uid)")
    check_content_same "addrbook_delete_master" $master 0 $master_space_0_content
    check_content_same "addrbook_delete_replica" $replica 0 $replica_space_0_content
}

test_dryrun_mode() {
    restore_configuration $(mk_extra_conf_test)

    _echo 'Dryrun configuration successfully installed! Trying to override addrbook handlers...'
    send_req_no_error_both "lua addrbook_enable_resharding()"

    _echo 'Congratulations! Resharding successfully enabled in Dryrun mode! Running tests...'
    test_dryrun_mode_impl

    _echo 'Surprise! Tests are done O_o'
}

get_lsn() {
    local t=$1
    send_req $t 'show info' | perl -pe 's/.*\blsn:\s*(\d+).*/$1/'
}

dump_master_lsn() {
    _echo "Trying to dump LSN number into $LSN_number_dump_file"

    local lsn=$(get_lsn $master)
    echo $lsn > $LSN_number_dump_file
    if [ $? -ne 0 ]; then
        continue_or_die "Can't store LSN into $LSN_number_dump_file. LSN in $lsn. Continue?"
    else
        _echo "LSN successfully stored into $LSN_number_dump_file. LSN is $lsn"
    fi

    echo $lsn
}

wait_replica_sync() {
    local master_lsn=$1
    local replica_lsn=-1

    local n_iters=0

    while [ $replica_lsn -lt $master_lsn ]; do
        [[ $replica_lsn -ne -1 ]] && sleep $LSN_request_timeout

        if [ $n_iters -ge 10 ]; then
            _echo "Too long Replica's LSN request... Current LSN == $replica_lsn."
            n_iters=0
        fi

        replica_lsn=$(get_lsn $replica)
        _echo "LSN: $replica_lsn"

        ((n_iters++))
    done
}

###########################################################################
# Check tarantool is ready
###########################################################################

_echo "Trying to check tarantools..."
is_ready $master_host $master_admin_port
is_ready $replica_host $replica_admin_port

###########################################################################
# Enable Dryrun mode for both tarantools and run tests
###########################################################################

_echo "Trying to run test script..."
if [ "$test_uid" == "" ]; then
    _echo "Test uid is not set! Dryrun mode is disabled! Can't test resharding!"
    continue_or_die "Are you sure you want to continue without testing?"
fi

if [ "$test_uid" != "" ]; then
    test_dryrun_mode
fi

#_echo "Trying to disable Dryrun resharding..."
#send_req $master 'lua addrbook_disable_resharding()'
#send_req $replica 'lua addrbook_disable_resharding()'

###########################################################################
# Dump master's LSN into a file and store it locally to wait replica's sync
###########################################################################

lsn=$(dump_master_lsn)

###########################################################################
# Enable resharding on Master and on Replica. Replica is still read-only, so
# modify requests on replica will fail...
# XXX: Dryrun mode is disabled here
###########################################################################

_echo "Trying to enable resharding on master..."
restore_configuration $(mk_extra_conf_common)
send_req_no_error $master "lua addrbook_enable_resharding()"

###########################################################################
# Update MySQL tables and wait until replica not syncked
###########################################################################

_echo "Trying to replace shards configuration in MySQL and wait for sync..."
update_mysql_configuration
wait_replica_sync $lsn

###########################################################################
# Disable replica's replication. Resharding will be enabled here. Dryrun is disabled
###########################################################################

_echo "Trying to enable resharding on replica..."
send_req_no_error $replica "lua addrbook_enable_resharding()"
replica_disable_replication

###########################################################################
# Replication should work here. Possibly we should start reloading of caprons here
###########################################################################
