# Oracle Source

The Oracle Source connector streams committed row changes (`INSERT`, `UPDATE`, `DELETE`) from Oracle Database into Apache Iggy through **XStream Out**. An outbound server inside the database captures changes from the redo log, assembles whole transactions and delivers them in commit order. The connector is the client of that outbound server.

> **Licence.** Per Oracle documentation, the XStream APIs are available with the Oracle GoldenGate product. Every user of this connector needs it. The Oracle client library is loaded at run time and is never redistributed with Iggy.

Target: Oracle 19c Enterprise Edition and later, multitenant (CDB) and non-CDB.

## Requirements

- An outbound server created by a DBA (see [Database setup](#database-setup)). The connector never runs DDL.
- Oracle Instant Client (Basic) on the host that runs the connectors runtime, and `libaio` on Linux. `oci_library_path` points at `libclntsh`; the platform default name is used when it is absent.
- `DIAG_SIGHANDLER_ENABLED=FALSE` in the client's `sqlnet.ora` (found through `TNS_ADMIN` or the client's `network/admin` directory). Without it the first OCI call installs signal handlers for `SIGINT`, `SIGQUIT`, `SIGILL`, `SIGTRAP`, `SIGABRT`, `SIGFPE`, `SIGXCPU`, `SIGXFSZ` and `SIGSYS` in the runtime process.
- Character conversion does not depend on `NLS_LANG`: the connector asks the client for AL32UTF8.

## Configuration

- `connect_string`: Easy Connect string of the CDB root service, for example `dbhost:1521/ORCLCDB`.
- `username`, `password`: the user that ran `CREATE_OUTBOUND`. The password is a secret and is not logged.
- `outbound_server`: the outbound server name, at most 30 bytes.
- `oci_library_path`: optional path of `libclntsh`.
- `tables`: optional list of `OWNER.TABLE` names or `OWNER.*`, which also covers tables created later. Absent or empty forwards every table the outbound server captures.
- `poll_interval`: pause before each poll. Defaults to `"100ms"`.
- `poll_budget`: time a poll may spend collecting a batch when the stream is busy. Defaults to `"500ms"`. An idle receive blocks for 1 to 2 seconds regardless.
- `max_batch_events`: changes (rows and schema changes) per batch. Defaults to `1000`.
- `reattach_backoff`, `max_reattach_attempts`: pause and attempt limit for a stale session (`ORA-26812`) or an unreachable database. Default `"5s"` and `5`.
- `verbose_logging`: log every batch.

```toml
[plugin_config]
connect_string = "dbhost:1521/ORCLCDB"
username = "c##xstrmadmin"
password = "change-me"
outbound_server = "IGGY_XOUT"
tables = ["APP.ORDERS"]
```

## Database setup

Run as a DBA, once. The sketch below is the 19c form.

```sql
-- in the CDB root
ALTER SYSTEM SET streams_pool_size = 300M;
EXEC DBMS_XSTREAM_AUTH.GRANT_ADMIN_PRIVILEGE(grantee => 'C##XSTRMADMIN', privilege_type => 'CAPTURE', grant_select_privileges => TRUE, container => 'ALL');
-- as C##XSTRMADMIN, with no long transaction open: CREATE_OUTBOUND waits for them
BEGIN
  DBMS_XSTREAM_ADM.CREATE_OUTBOUND(server_name => 'IGGY_XOUT', source_database => 'PDB1', schema_names => 'APP');
END;
/
```

The database must be in `ARCHIVELOG` mode, and the captured tables need supplemental logging (`ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS` for full before images on updates). A schema rule is required for new tables to be captured: a table rule misses `CREATE TABLE` of new tables and stops following a renamed table.

## Delivery

- Order follows the commit order of Oracle only within one partition. Set `partition_id = 0` on the stream (see the example config), or use a topic with one partition. Without it the runtime spreads messages over the partitions of an existing topic, and the commit order is lost. One Iggy message is produced per row change, with the payload:

```json
{
  "table": "APP.ORDERS",
  "op": "update",
  "position": "00000000001934F7...",
  "xid": "10.25.831",
  "source_time": "2026-11-25T10:11:12",
  "before": {"ID": 7, "STATUS": "NEW"},
  "after": {"ID": 7, "STATUS": "SHIPPED"}
}
```

- Headers `table` and `op` carry the table name and the operation (`insert`, `update`, `delete` or `ddl`).
- A DDL statement on a captured table never stops the connector. `COMMENT`, `CREATE INDEX` and `GRANT OBJECT` are skipped. Everything else is forwarded in stream order as one message with `op` `ddl`, the Oracle `command` and the `statement` text, and without `before` or `after`. This covers `ALTER TABLE` (add, modify, rename or drop column), `CREATE TABLE`, `TRUNCATE TABLE`, `DROP TABLE` and `RENAME`.
- Row messages carry exactly the columns of their LCR, so apply a schema change to the target when its message arrives. Stopping on DDL is not offered because Oracle replays the DDL record after a restart.
- Delivery is at least once. A batch is staged until the runtime acknowledges it. On acknowledgement the connector commits its checkpoint and tells Oracle the processed low position, which lets the database purge redo it no longer needs. On a negative acknowledgement the same batch is sent again from memory without detaching from the outbound server.
- The message id is derived from the transaction id and the number of the change inside the transaction. The number keeps counting when a large transaction spans several batches, and a replay carries the same ids.
- A batch that fills in the middle of a transaction ends there. Acknowledging it does not move the low position into the open transaction, so a crash replays that whole transaction.
- Unacknowledged transactions are replayed in full after a restart, a killed runtime or a lost session.
- If Oracle reports that the saved position is below its processed low position (`ORA-21560`), the connector attaches once more without a position, so the outbound server picks its own start. Changes between the two positions are not replayed.

## State

The checkpoint holds a version, the outbound server name, the opaque position of the last acknowledged commit and a counter. A checkpoint written for another outbound server or another version fails `open`. Undecodable state starts from the outbound server's own position.

## Limits

- `NUMBER`, `VARCHAR2`, `NVARCHAR2`, `CHAR`, `NCHAR`, `DATE`, `TIMESTAMP` (with and without time zone), `RAW`, `BINARY_FLOAT` and `BINARY_DOUBLE` are forwarded. A forwarded row with any other column type (LOB, `LONG`, `XMLType`, `INTERVAL`, `ROWID`) ends the poll there. The commits read before it are delivered and acknowledged, then the connector stops with an error that names the table and column.
- The changes of the transaction that holds the row are not delivered, and they are replayed when the connector is started again after the column is removed or the table is excluded.
- Rows of tables excluded by `tables` are skipped and never stop the connector.
- There is no initial snapshot: the stream starts at the outbound server's position.
- Not tested: Standard Edition 2, RAC, Data Guard, managed cloud databases.

## Tests

`cargo nextest run -p iggy_connector_oracle_xstream_source --lib` needs no Oracle. `tests/smoke.rs` runs against a real outbound server and is ignored by default:

```bash
ORACLE_XSTREAM_CONNECT=dbhost:1521/ORCLCDB ORACLE_XSTREAM_USER=c##xstrmadmin ORACLE_XSTREAM_PASSWORD=... \
ORACLE_XSTREAM_SERVER=IGGY_XOUT ORACLE_XSTREAM_LIBRARY=/opt/oracle/instantclient/libclntsh.so \
cargo test -p iggy_connector_oracle_xstream_source --test smoke -- --ignored
```
