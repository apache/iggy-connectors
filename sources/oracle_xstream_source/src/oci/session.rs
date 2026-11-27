// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::ptr::{null, null_mut};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::loader::{Client, Handle, Sword, Text};
use super::value::{
    DateTimeParts, SQLT_TIMESTAMP_TZ, decode_date, decode_scalar, format_timestamp,
    is_datetime_descriptor,
};
use crate::stream::{
    Column, DdlChange, LcrStream, Operation, Received, RowChange, StreamError, Value,
};

const OCI_SUCCESS: Sword = 0;
const OCI_SUCCESS_WITH_INFO: Sword = 1;
const OCI_NO_DATA: Sword = 100;
const OCI_STILL_EXECUTING: Sword = -3123;
const OCI_DEFAULT: u32 = 0;
const OCI_THREADED: u32 = 1;
const OCI_OBJECT: u32 = 2;
const HTYPE_ENV: u32 = 1;
const HTYPE_ERROR: u32 = 2;
const HTYPE_SVCCTX: u32 = 3;
const HTYPE_SERVER: u32 = 8;
const HTYPE_SESSION: u32 = 9;
const ATTR_SERVER: u32 = 6;
const ATTR_SESSION: u32 = 7;
const ATTR_USERNAME: u32 = 22;
const ATTR_PASSWORD: u32 = 23;
const CRED_RDBMS: u32 = 1;
const LCR_XDDL: u8 = 4;
const COLUMN_VALUE_OLD: u16 = 0;
const COLUMN_VALUE_NEW: u16 = 1;
const MORE_ROW_DATA: u64 = 1;
/// Oracle character set id of AL32UTF8. Passing it to `OCIEnvNlsCreate` makes the client independent of `NLS_LANG`.
const CHARSET_AL32UTF8: u16 = 873;
const POSITION_CAPACITY: usize = 256;
const ERROR_MESSAGE_CAPACITY: usize = 1024;
/// Oracle allows at most 1000 columns per table.
const MAX_COLUMNS: usize = 1000;
const OCI_DATE_BUFFER: usize = 16;
const DETACH_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

const COMMAND_COMMIT: &str = "COMMIT";
const COMMAND_INSERT: &str = "INSERT";
const COMMAND_UPDATE: &str = "UPDATE";
const COMMAND_DELETE: &str = "DELETE";

#[derive(Debug, Clone)]
pub struct ConnectParams {
    pub connect_string: String,
    pub username: String,
    pub password: String,
    pub outbound_server: String,
}

/// One attached XStream Out session. All raw handles are owned here and released in `Drop`.
///
/// OCI calls are made from whichever blocking thread the caller picks, one call at a time: the source moves the
/// session out of its mutex for the duration of a call, so two threads never use the handles together.
pub struct Session {
    client: Arc<Client>,
    env: Handle,
    error: Handle,
    server: Handle,
    service: Handle,
    user: Handle,
    server_attached: bool,
    session_begun: bool,
    buffers: ColumnBuffers,
}

// SAFETY: the handles are plain OCI pointers with no thread affinity when the environment is created with
// OCI_THREADED, and the owner never uses them from two threads at once (see the type documentation).
unsafe impl Send for Session {}

struct ColumnBuffers {
    names: Vec<Text>,
    name_lengths: Vec<u16>,
    dtypes: Vec<u16>,
    values: Vec<Handle>,
    indicators: Vec<i16>,
    lengths: Vec<u16>,
    charset_forms: Vec<u8>,
    flags: Vec<u64>,
    charset_ids: Vec<u16>,
}

impl ColumnBuffers {
    fn new() -> Self {
        Self {
            names: vec![null_mut(); MAX_COLUMNS],
            name_lengths: vec![0; MAX_COLUMNS],
            dtypes: vec![0; MAX_COLUMNS],
            values: vec![null_mut(); MAX_COLUMNS],
            indicators: vec![0; MAX_COLUMNS],
            lengths: vec![0; MAX_COLUMNS],
            charset_forms: vec![0; MAX_COLUMNS],
            flags: vec![0; MAX_COLUMNS],
            charset_ids: vec![0; MAX_COLUMNS],
        }
    }
}

struct Header {
    command: String,
    owner: String,
    object: String,
    transaction_id: String,
    position: Vec<u8>,
    source_time: Option<String>,
}

impl Session {
    /// Connects and attaches to the outbound server. `position` is the saved position, or `None` to let the
    /// server derive the start from its processed low position.
    pub fn connect(
        client: Arc<Client>,
        params: &ConnectParams,
        position: Option<&[u8]>,
    ) -> Result<Self, StreamError> {
        let mut session = Self {
            client,
            env: null_mut(),
            error: null_mut(),
            server: null_mut(),
            service: null_mut(),
            user: null_mut(),
            server_attached: false,
            session_begun: false,
            buffers: ColumnBuffers::new(),
        };
        session.open(params, position)?;
        Ok(session)
    }

    fn open(&mut self, params: &ConnectParams, position: Option<&[u8]>) -> Result<(), StreamError> {
        let client = Arc::clone(&self.client);
        // SAFETY: every pointer passed below is either null where OCI documents null, or a handle allocated
        // in this function, or a buffer that outlives the call.
        unsafe {
            let rc = (client.env_nls_create)(
                &mut self.env,
                OCI_THREADED | OCI_OBJECT,
                null_mut(),
                null_mut(),
                null_mut(),
                null_mut(),
                0,
                null_mut(),
                CHARSET_AL32UTF8,
                CHARSET_AL32UTF8,
            );
            if rc != OCI_SUCCESS && rc != OCI_SUCCESS_WITH_INFO {
                return Err(StreamError::Oci {
                    operation: "OCIEnvNlsCreate",
                    code: rc,
                    message: "cannot create the OCI environment (check the client installation)"
                        .to_owned(),
                });
            }
            for (handle, kind) in [
                (&mut self.error, HTYPE_ERROR),
                (&mut self.server, HTYPE_SERVER),
                (&mut self.service, HTYPE_SVCCTX),
                (&mut self.user, HTYPE_SESSION),
            ] {
                let rc = (client.handle_alloc)(self.env, handle, kind, 0, null_mut());
                if rc != OCI_SUCCESS {
                    return Err(StreamError::Oci {
                        operation: "OCIHandleAlloc",
                        code: rc,
                        message: format!("handle type {kind}"),
                    });
                }
            }
            let connect = params.connect_string.as_bytes();
            self.check(
                (client.server_attach)(
                    self.server,
                    self.error,
                    connect.as_ptr(),
                    connect.len() as i32,
                    OCI_DEFAULT,
                ),
                "OCIServerAttach",
            )?;
            self.server_attached = true;
            self.check(
                (client.attr_set)(
                    self.service,
                    HTYPE_SVCCTX,
                    self.server,
                    0,
                    ATTR_SERVER,
                    self.error,
                ),
                "OCIAttrSet server",
            )?;
            for (kind, text) in [
                (ATTR_USERNAME, &params.username),
                (ATTR_PASSWORD, &params.password),
            ] {
                self.check(
                    (client.attr_set)(
                        self.user,
                        HTYPE_SESSION,
                        text.as_ptr() as Handle,
                        text.len() as u32,
                        kind,
                        self.error,
                    ),
                    "OCIAttrSet credentials",
                )?;
            }
            self.check(
                (client.session_begin)(
                    self.service,
                    self.error,
                    self.user,
                    CRED_RDBMS,
                    OCI_DEFAULT,
                ),
                "OCISessionBegin",
            )?;
            self.session_begun = true;
            self.check(
                (client.attr_set)(
                    self.service,
                    HTYPE_SVCCTX,
                    self.user,
                    0,
                    ATTR_SESSION,
                    self.error,
                ),
                "OCIAttrSet session",
            )?;
            let name = params.outbound_server.as_bytes();
            let (position_ptr, position_len) = match position {
                Some(bytes) => (bytes.as_ptr(), bytes.len() as u16),
                None => (null(), 0),
            };
            self.check(
                (client.out_attach)(
                    self.service,
                    self.error,
                    name.as_ptr(),
                    name.len() as u16,
                    position_ptr,
                    position_len,
                    OCI_DEFAULT,
                ),
                "OCIXStreamOutAttach",
            )
        }
    }

    fn check(&self, rc: Sword, operation: &'static str) -> Result<(), StreamError> {
        if rc == OCI_SUCCESS || rc == OCI_SUCCESS_WITH_INFO {
            return Ok(());
        }
        let mut code = 0i32;
        let mut message = [0u8; ERROR_MESSAGE_CAPACITY];
        // SAFETY: `error` is a live error handle and `message` is a writable buffer of the stated size.
        unsafe {
            (self.client.error_get)(
                self.error,
                1,
                null_mut(),
                &mut code,
                message.as_mut_ptr(),
                message.len() as u32,
                HTYPE_ERROR,
            );
        }
        let end = message
            .iter()
            .position(|&byte| byte == 0)
            .unwrap_or(message.len());
        Err(StreamError::Oci {
            operation,
            code,
            message: String::from_utf8_lossy(&message[..end]).trim().to_owned(),
        })
    }

    fn read_ddl_statement(&self, lcr: Handle) -> Result<String, StreamError> {
        let mut statement: Text = null_mut();
        let mut statement_len = 0u32;
        let (mut object_type, mut logon_user, mut current_schema): (Text, Text, Text) =
            (null_mut(), null_mut(), null_mut());
        let (mut base_owner, mut base_name): (Text, Text) = (null_mut(), null_mut());
        let (mut object_type_len, mut logon_user_len, mut current_schema_len) = (0u16, 0u16, 0u16);
        let (mut base_owner_len, mut base_name_len) = (0u16, 0u16);
        let mut flag = 0u64;
        // SAFETY: all out parameters point at live locals; the statement pointer is copied into an owned string
        // below before the next OCI call can invalidate it.
        let rc = unsafe {
            (self.client.ddl_info_get)(
                self.service,
                self.error,
                &mut object_type,
                &mut object_type_len,
                &mut statement,
                &mut statement_len,
                &mut logon_user,
                &mut logon_user_len,
                &mut current_schema,
                &mut current_schema_len,
                &mut base_owner,
                &mut base_owner_len,
                &mut base_name,
                &mut base_name_len,
                &mut flag,
                lcr,
                OCI_DEFAULT,
            )
        };
        self.check(rc, "OCILCRDDLInfoGet")?;
        // SAFETY: the pair was just filled in by OCILCRDDLInfoGet.
        Ok(unsafe { text_long(statement, statement_len) })
    }

    fn read_header(&self, lcr: Handle) -> Result<Header, StreamError> {
        let nulls = || null_mut::<u8>();
        let (
            mut source_db,
            mut command,
            mut owner,
            mut object,
            mut tag,
            mut transaction,
            mut position,
        ) = (
            nulls(),
            nulls(),
            nulls(),
            nulls(),
            nulls(),
            nulls(),
            nulls(),
        );
        let (mut source_db_len, mut command_len, mut owner_len, mut object_len) =
            (0u16, 0u16, 0u16, 0u16);
        let (mut tag_len, mut transaction_len, mut position_len) = (0u16, 0u16, 0u16);
        let (mut old_columns, mut new_columns) = (0u16, 0u16);
        let mut date = [0u8; OCI_DATE_BUFFER];
        let mut flags = 0u64;
        // SAFETY: all out parameters point at live locals; the text pointers returned by OCI are copied
        // into owned values below before the next OCI call can invalidate them.
        let rc = unsafe {
            (self.client.header_get)(
                self.service,
                self.error,
                &mut source_db,
                &mut source_db_len,
                &mut command,
                &mut command_len,
                &mut owner,
                &mut owner_len,
                &mut object,
                &mut object_len,
                &mut tag,
                &mut tag_len,
                &mut transaction,
                &mut transaction_len,
                date.as_mut_ptr() as Handle,
                &mut old_columns,
                &mut new_columns,
                &mut position,
                &mut position_len,
                &mut flags,
                lcr,
                OCI_DEFAULT,
            )
        };
        self.check(rc, "OCILCRHeaderGet")?;
        // SAFETY: each (pointer, length) pair was just filled in by OCILCRHeaderGet.
        unsafe {
            Ok(Header {
                command: text(command, command_len),
                owner: text(owner, owner_len),
                object: text(object, object_len),
                transaction_id: text(transaction, transaction_len),
                position: bytes(position, position_len),
                source_time: decode_date(&date),
            })
        }
    }

    fn read_columns(&mut self, lcr: Handle, value_type: u16) -> Result<Vec<Column>, StreamError> {
        let mut count = 0u16;
        let buffers = &mut self.buffers;
        // SAFETY: every array has MAX_COLUMNS entries, the array size passed matches, and `lcr` is the LCR
        // returned by the receive that is still active.
        let rc = unsafe {
            (self.client.row_column_info_get)(
                self.service,
                self.error,
                value_type,
                &mut count,
                buffers.names.as_mut_ptr(),
                buffers.name_lengths.as_mut_ptr(),
                buffers.dtypes.as_mut_ptr(),
                buffers.values.as_mut_ptr(),
                buffers.indicators.as_mut_ptr(),
                buffers.lengths.as_mut_ptr(),
                buffers.charset_forms.as_mut_ptr(),
                buffers.flags.as_mut_ptr(),
                buffers.charset_ids.as_mut_ptr(),
                lcr,
                MAX_COLUMNS as u16,
                OCI_DEFAULT,
            )
        };
        self.check(rc, "OCILCRRowColumnInfoGet")?;
        let mut columns = Vec::with_capacity(usize::from(count));
        for index in 0..usize::from(count).min(MAX_COLUMNS) {
            // SAFETY: index < count, and OCI filled the first `count` entries of each array.
            let name = unsafe { text(self.buffers.names[index], self.buffers.name_lengths[index]) };
            let value = self.column_value(index);
            columns.push(Column { name, value });
        }
        Ok(columns)
    }

    fn column_value(&self, index: usize) -> Value {
        let buffers = &self.buffers;
        if buffers.indicators[index] != 0 {
            return Value::Null;
        }
        let dtype = buffers.dtypes[index];
        let data = buffers.values[index];
        if is_datetime_descriptor(dtype) {
            return self.datetime_value(dtype, data);
        }
        let raw = if data.is_null() {
            &[][..]
        } else {
            // SAFETY: OCI reports `lengths[index]` readable bytes at `values[index]`, valid until the next receive.
            unsafe {
                std::slice::from_raw_parts(data as *const u8, usize::from(buffers.lengths[index]))
            }
        };
        decode_scalar(dtype, raw).unwrap_or(Value::Unsupported)
    }

    fn datetime_value(&self, dtype: u16, descriptor: Handle) -> Value {
        let (mut year, mut month, mut day) = (0i16, 0u8, 0u8);
        let (mut hour, mut minute, mut second, mut nanoseconds) = (0u8, 0u8, 0u8, 0u32);
        let (mut offset_hours, mut offset_minutes) = (0i8, 0i8);
        // SAFETY: `descriptor` is a datetime descriptor owned by the active LCR; the outputs are live locals.
        let (date_rc, time_rc, offset_rc) = unsafe {
            let date_rc = (self.client.date_time_get_date)(
                self.env, self.error, descriptor, &mut year, &mut month, &mut day,
            );
            let time_rc = (self.client.date_time_get_time)(
                self.env,
                self.error,
                descriptor,
                &mut hour,
                &mut minute,
                &mut second,
                &mut nanoseconds,
            );
            let offset_rc = if dtype == SQLT_TIMESTAMP_TZ {
                (self.client.date_time_get_offset)(
                    self.env,
                    self.error,
                    descriptor,
                    &mut offset_hours,
                    &mut offset_minutes,
                )
            } else {
                OCI_SUCCESS
            };
            (date_rc, time_rc, offset_rc)
        };
        if [date_rc, time_rc, offset_rc]
            .iter()
            .any(|rc| *rc != OCI_SUCCESS)
        {
            return Value::Unsupported;
        }
        Value::Timestamp(format_timestamp(&DateTimeParts {
            year,
            month,
            day,
            hour,
            minute,
            second,
            nanoseconds,
            offset: (dtype == SQLT_TIMESTAMP_TZ).then_some((offset_hours, offset_minutes)),
        }))
    }

    /// Reads every pending chunk of an LCR that carries LOB, LONG or XMLType data. v1 does not forward those
    /// values, so only the column names are kept, to report them.
    fn drain_chunks(&self) -> Result<Vec<String>, StreamError> {
        let mut names = Vec::new();
        loop {
            let (mut name, mut data): (Text, Text) = (null_mut(), null_mut());
            let (mut name_len, mut dtype, mut charset_id) = (0u16, 0u16, 0u16);
            let (mut flags, mut more) = (0u64, 0u64);
            let mut length = 0u32;
            // SAFETY: all out parameters are live locals, and the receive that produced this LCR is active.
            let rc = unsafe {
                (self.client.chunk_receive)(
                    self.service,
                    self.error,
                    &mut name,
                    &mut name_len,
                    &mut dtype,
                    &mut flags,
                    &mut charset_id,
                    &mut length,
                    &mut data,
                    &mut more,
                    OCI_DEFAULT,
                )
            };
            if rc != OCI_STILL_EXECUTING {
                self.check(rc, "OCIXStreamOutChunkReceive")?;
            }
            // SAFETY: `name` and `name_len` were just filled in by the chunk receive.
            let column = unsafe { text(name, name_len) };
            if !column.is_empty() && !names.contains(&column) {
                names.push(column);
            }
            if more & MORE_ROW_DATA == 0 {
                return Ok(names);
            }
        }
    }

    fn row_change(
        &mut self,
        header: Header,
        lcr: Handle,
        more_row_data: bool,
    ) -> Result<RowChange, StreamError> {
        let operation = match header.command.as_str() {
            COMMAND_INSERT => Operation::Insert,
            COMMAND_UPDATE => Operation::Update,
            _ => Operation::Delete,
        };
        // OCI rejects the other side: an INSERT has only NEW values and a DELETE only OLD values, whatever the
        // header column counts say.
        let mut old = match operation {
            Operation::Insert => Vec::new(),
            Operation::Update | Operation::Delete => self.read_columns(lcr, COLUMN_VALUE_OLD)?,
        };
        let mut new = match operation {
            Operation::Delete => Vec::new(),
            Operation::Insert | Operation::Update => self.read_columns(lcr, COLUMN_VALUE_NEW)?,
        };
        if more_row_data {
            for name in self.drain_chunks()? {
                mark_unsupported(&mut old, &name);
                mark_unsupported(&mut new, &name);
                if operation == Operation::Delete {
                    continue;
                }
                if !new.iter().any(|column| column.name == name) {
                    new.push(Column {
                        name,
                        value: Value::Unsupported,
                    });
                }
            }
        }
        Ok(RowChange {
            table: format!("{}.{}", header.owner, header.object),
            operation,
            transaction_id: header.transaction_id,
            position: header.position,
            source_time: header.source_time,
            old,
            new,
            ordinal: 0,
        })
    }
}

impl LcrStream for Session {
    fn receive(&mut self) -> Result<Received, StreamError> {
        let mut lcr: Handle = null_mut();
        let (mut lcr_type, mut flags) = (0u8, 0u64);
        let mut fetch_position = [0u8; POSITION_CAPACITY];
        let mut fetch_len = POSITION_CAPACITY as u16;
        // SAFETY: the out parameters are live locals and the position buffer has the stated capacity.
        let rc = unsafe {
            (self.client.out_receive)(
                self.service,
                self.error,
                &mut lcr,
                &mut lcr_type,
                &mut flags,
                fetch_position.as_mut_ptr(),
                &mut fetch_len,
                OCI_DEFAULT,
            )
        };
        let ok = rc == OCI_SUCCESS || rc == OCI_SUCCESS_WITH_INFO;
        // OCI_STILL_EXECUTING arrives together with an LCR and means the receive stays active;
        // success with no LCR is the idle return.
        if rc != OCI_STILL_EXECUTING && lcr.is_null() && (ok || rc == OCI_NO_DATA) {
            return Ok(Received::Idle);
        }
        if rc != OCI_STILL_EXECUTING {
            self.check(rc, "OCIXStreamOutLCRReceive")?;
        }
        let header = self.read_header(lcr)?;
        if lcr_type == LCR_XDDL {
            return Ok(Received::Ddl(DdlChange {
                table: format!("{}.{}", header.owner, header.object),
                command: header.command,
                statement: self.read_ddl_statement(lcr)?,
                transaction_id: header.transaction_id,
                position: header.position,
                source_time: header.source_time,
                ordinal: 0,
            }));
        }
        match header.command.as_str() {
            COMMAND_COMMIT => Ok(Received::Commit {
                position: header.position,
            }),
            COMMAND_INSERT | COMMAND_UPDATE | COMMAND_DELETE => self
                .row_change(header, lcr, flags & MORE_ROW_DATA != 0)
                .map(Received::Row),
            other => Err(StreamError::Protocol(format!("row LCR command {other:?}"))),
        }
    }

    fn set_processed_low_position(&mut self, position: &[u8]) -> Result<(), StreamError> {
        // SAFETY: `position` is a readable buffer of the stated length for the duration of the call.
        let rc = unsafe {
            (self.client.out_lwm_set)(
                self.service,
                self.error,
                position.as_ptr(),
                position.len() as u16,
                OCI_DEFAULT,
            )
        };
        self.check(rc, "OCIXStreamOutProcessedLWMSet")
    }

    fn detach(&mut self) -> Result<(), StreamError> {
        let deadline = Instant::now() + DETACH_DRAIN_TIMEOUT;
        loop {
            match self.receive() {
                Ok(Received::Idle) => break,
                Ok(_) => {}
                Err(_) => break,
            }
            if Instant::now() >= deadline {
                return Err(StreamError::Protocol(
                    "the stream was still active at the detach deadline".to_owned(),
                ));
            }
        }
        // SAFETY: the service context and error handle are live and the receive is idle.
        let rc = unsafe { (self.client.out_detach)(self.service, self.error, OCI_DEFAULT) };
        self.check(rc, "OCIXStreamOutDetach")
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let client = &self.client;
        // SAFETY: each handle is released at most once, in reverse order of creation, and only if it was created.
        unsafe {
            if self.session_begun {
                (client.session_end)(self.service, self.error, self.user, OCI_DEFAULT);
            }
            if self.server_attached {
                (client.server_detach)(self.server, self.error, OCI_DEFAULT);
            }
            for (handle, kind) in [
                (self.service, HTYPE_SVCCTX),
                (self.user, HTYPE_SESSION),
                (self.server, HTYPE_SERVER),
                (self.error, HTYPE_ERROR),
                (self.env, HTYPE_ENV),
            ] {
                if !handle.is_null() {
                    (client.handle_free)(handle, kind);
                }
            }
        }
    }
}

fn mark_unsupported(columns: &mut [Column], name: &str) {
    for column in columns.iter_mut().filter(|column| column.name == name) {
        column.value = Value::Unsupported;
    }
}

/// # Safety
/// `pointer` must be null or point at `length` readable bytes.
unsafe fn bytes(pointer: Text, length: u16) -> Vec<u8> {
    if pointer.is_null() || length == 0 {
        return Vec::new();
    }
    // SAFETY: guaranteed by the caller.
    unsafe { std::slice::from_raw_parts(pointer, usize::from(length)) }.to_vec()
}

/// # Safety
/// `pointer` must be null or point at `length` readable bytes.
unsafe fn text_long(pointer: Text, length: u32) -> String {
    if pointer.is_null() || length == 0 {
        return String::new();
    }
    // SAFETY: guaranteed by the caller.
    let raw = unsafe { std::slice::from_raw_parts(pointer, length as usize) };
    // Measured on 19c: the reported length of a DDL statement includes the terminating NUL byte.
    String::from_utf8_lossy(raw)
        .trim_end_matches('\0')
        .to_owned()
}

/// # Safety
/// Same contract as [`bytes`].
unsafe fn text(pointer: Text, length: u16) -> String {
    // SAFETY: guaranteed by the caller.
    String::from_utf8_lossy(&unsafe { bytes(pointer, length) }).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_statement_with_terminating_nul_when_reading_should_drop_the_nul() {
        let mut raw = b"ALTER TABLE t ADD (c NUMBER)\0".to_vec();
        // SAFETY: the pointer and length describe the live vector.
        let statement = unsafe { text_long(raw.as_mut_ptr(), raw.len() as u32) };
        assert_eq!(statement, "ALTER TABLE t ADD (c NUMBER)");
    }

    #[test]
    fn given_null_pointer_when_reading_text_should_return_an_empty_string() {
        // SAFETY: a null pointer is allowed by the contract.
        assert_eq!(unsafe { text_long(null_mut(), 5) }, "");
    }
}
