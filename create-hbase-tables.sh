#!/bin/bash

function check_hbase_ready {
    echo "Checking if HBase is ready..."
    for i in {1..30}; do
        echo "status" | /opt/hbase/bin/hbase shell -n 2>/dev/null | grep -q "active master" && return 0
        sleep 5
    done
    return 1
}

if check_hbase_ready; then
    echo "HBase is ready!"
else
    echo "HBase did not start within the expected time." >&2
    exit 1
fi

echo "Creating HBase tables..."
echo "create 'blocks', 'x'" | /opt/hbase/bin/hbase shell -n
echo "create 'tx', 'x'" | /opt/hbase/bin/hbase shell -n
echo "create 'tx-by-addr', 'x'" | /opt/hbase/bin/hbase shell -n
echo "create 'tx_full', 'x'" | /opt/hbase/bin/hbase shell -n
echo "HBase tables created."

exit 0
