#!/bin/sh
# Scripted demo for `docker compose up`: writes to the source PostgreSQL like an application would, then asserts
# that every change reached the read model and the search index. Prints one JSON line and exits non-zero on failure.
set -eu

SRC="postgresql://shop_app:${SHOP_APP_PASSWORD}@source-pg:5432/shop"
SRC_ADMIN="postgresql://postgres:${SOURCE_ADMIN_PASSWORD}@source-pg:5432/shop"
RM="postgresql://postgres:${READMODEL_PASSWORD}@readmodel-pg:5432/readmodel"
MEILI="http://meilisearch:7700"
AUTH="Authorization: Bearer ${MEILI_MASTER_KEY}"
FAILURES=""

fail() { FAILURES="${FAILURES}\"$1\","; echo "demo: FAILED: $1"; }

rm_value() { psql "$RM" -tAc "$1" 2>/dev/null || true; }

# wait_until <description> <max seconds> <sql> <expected value>
wait_until() {
  i=0
  while [ "$(rm_value "$3")" != "$4" ]; do
    i=$((i + 1))
    if [ "$i" -gt $(($2 * 2)) ]; then fail "$1"; return 0; fi
    sleep 0.5
  done
  echo "demo: ok - $1"
}

echo "demo: waiting for the consumer and Debezium"
i=0
until wget -qO- http://consumer:8080/health/ready >/dev/null 2>&1; do
  i=$((i + 1)); [ "$i" -gt 240 ] && { fail "consumer ready"; break; }; sleep 0.5
done
# Debezium creates the replication slot and keeps it active while it streams.
i=0
until [ "$(psql "$SRC_ADMIN" -tAc "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'cdc_shop' AND active" 2>/dev/null)" = "1" ]; do
  i=$((i + 1)); [ "$i" -gt 240 ] && { fail "debezium streaming"; break; }; sleep 0.5
done

echo "demo: writing to the source database"
psql -v ON_ERROR_STOP=1 -q "$SRC" <<'SQL'
INSERT INTO customers (id, full_name, email, city) VALUES (900001, 'Deniz Yildiz', 'deniz@example.test', 'Ankara');
BEGIN;
INSERT INTO orders (id, customer_id, status, total_amount) VALUES (900001, 900001, 'NEW', 31.50);
INSERT INTO order_lines (order_id, line_no, sku, quantity, unit_price) VALUES (900001, 1, 'TEA-500', 1, 10.50), (900001, 2, 'COFFEE-250', 2, 10.50);
COMMIT;
UPDATE customers SET city = 'Izmir', version = version + 1, updated_at = clock_timestamp() WHERE id = 900001;
DELETE FROM order_lines WHERE order_id = 900001 AND line_no = 2;
SQL

wait_until "customer update in read model" 60 "SELECT city FROM rm_customer WHERE source = 'pg' AND id = 900001" "Izmir"
wait_until "line delete kept as tombstone" 60 "SELECT deleted FROM rm_order_line WHERE source = 'pg' AND order_id = 900001 AND line_no = 2" "t"
wait_until "order transaction complete (3 events)" 60 \
  "SELECT count(*) FROM rm_tx t JOIN rm_order o ON o.tx_id = t.tx_id AND o.source = t.source WHERE o.id = 900001 AND t.expected_events = 3 AND t.completed_at IS NOT NULL" "1"

echo "demo: schema evolution - ADD COLUMN loyalty_tier"
psql -v ON_ERROR_STOP=1 -q "$SRC_ADMIN" -c "ALTER TABLE customers ADD COLUMN IF NOT EXISTS loyalty_tier text"
psql -v ON_ERROR_STOP=1 -q "$SRC" -c "UPDATE customers SET loyalty_tier = 'GOLD', version = version + 1, updated_at = clock_timestamp() WHERE id = 900001"
wait_until "added column projected" 60 "SELECT loyalty_tier FROM rm_customer WHERE source = 'pg' AND id = 900001" "GOLD"
wait_until "every change applied once" 30 "SELECT apply_count = version FROM rm_customer WHERE source = 'pg' AND id = 900001" "t"
wait_until "search index caught up" 60 \
  "SELECT (SELECT count(*) FROM rm_customer WHERE index_dirty) + (SELECT count(*) FROM rm_order_line WHERE index_dirty)" "0"

doc=$(wget -qO- --header "$AUTH" "$MEILI/indexes/customers/documents/pg-900001" 2>/dev/null | tr -d '\n' || true)
case "$doc" in
  *'"loyalty_tier":"GOLD"'*'"city":"Izmir"'* | *'"city":"Izmir"'*'"loyalty_tier":"GOLD"'*) echo "demo: ok - customer document searchable with new column" ;;
  *) fail "customer document in search" ;;
esac
if wget -qO- --header "$AUTH" "$MEILI/indexes/order_lines/documents/pg-900001-2" >/dev/null 2>&1; then
  fail "deleted line removed from search"
else
  echo "demo: ok - deleted line removed from search"
fi

applied=$(rm_value "SELECT value FROM rm_counter WHERE name = 'events_applied'")
failures="[${FAILURES%,}]"
ok=false
[ "$failures" = "[]" ] && ok=true
printf '{"ok":%s,"failures":%s,"eventsApplied":%s,"searchDocument":%s}\n' "$ok" "$failures" "${applied:-0}" "${doc:-null}"
[ "$ok" = true ]
