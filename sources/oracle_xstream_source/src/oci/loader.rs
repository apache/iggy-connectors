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

use std::ffi::c_void;

use libloading::Library;

use crate::stream::StreamError;

pub type Sword = i32;
pub type Handle = *mut c_void;
pub type Text = *mut u8;

type EnvNlsCreate = unsafe extern "C" fn(
    *mut Handle,
    u32,
    Handle,
    Handle,
    Handle,
    Handle,
    usize,
    *mut Handle,
    u16,
    u16,
) -> Sword;
type HandleAlloc = unsafe extern "C" fn(Handle, *mut Handle, u32, usize, *mut Handle) -> Sword;
type HandleFree = unsafe extern "C" fn(Handle, u32) -> Sword;
type ServerAttach = unsafe extern "C" fn(Handle, Handle, *const u8, i32, u32) -> Sword;
type ServerDetach = unsafe extern "C" fn(Handle, Handle, u32) -> Sword;
type AttrSet = unsafe extern "C" fn(Handle, u32, Handle, u32, u32, Handle) -> Sword;
type SessionBegin = unsafe extern "C" fn(Handle, Handle, Handle, u32, u32) -> Sword;
type SessionEnd = unsafe extern "C" fn(Handle, Handle, Handle, u32) -> Sword;
type ErrorGet = unsafe extern "C" fn(Handle, u32, *mut u8, *mut i32, *mut u8, u32, u32) -> Sword;
type OutAttach = unsafe extern "C" fn(Handle, Handle, *const u8, u16, *const u8, u16, u32) -> Sword;
type OutReceive = unsafe extern "C" fn(
    Handle,
    Handle,
    *mut Handle,
    *mut u8,
    *mut u64,
    *mut u8,
    *mut u16,
    u32,
) -> Sword;
type OutLwmSet = unsafe extern "C" fn(Handle, Handle, *const u8, u16, u32) -> Sword;
type OutDetach = unsafe extern "C" fn(Handle, Handle, u32) -> Sword;
type ChunkReceive = unsafe extern "C" fn(
    Handle,
    Handle,
    *mut Text,
    *mut u16,
    *mut u16,
    *mut u64,
    *mut u16,
    *mut u32,
    *mut Text,
    *mut u64,
    u32,
) -> Sword;
type RowColumnInfoGet = unsafe extern "C" fn(
    Handle,
    Handle,
    u16,
    *mut u16,
    *mut Text,
    *mut u16,
    *mut u16,
    *mut Handle,
    *mut i16,
    *mut u16,
    *mut u8,
    *mut u64,
    *mut u16,
    Handle,
    u16,
    u32,
) -> Sword;
type HeaderGet = unsafe extern "C" fn(
    Handle,
    Handle,
    *mut Text,
    *mut u16,
    *mut Text,
    *mut u16,
    *mut Text,
    *mut u16,
    *mut Text,
    *mut u16,
    *mut Text,
    *mut u16,
    *mut Text,
    *mut u16,
    Handle,
    *mut u16,
    *mut u16,
    *mut Text,
    *mut u16,
    *mut u64,
    Handle,
    u32,
) -> Sword;
type DdlInfoGet = unsafe extern "C" fn(
    Handle,
    Handle,
    *mut Text,
    *mut u16,
    *mut Text,
    *mut u32,
    *mut Text,
    *mut u16,
    *mut Text,
    *mut u16,
    *mut Text,
    *mut u16,
    *mut Text,
    *mut u16,
    *mut u64,
    Handle,
    u32,
) -> Sword;
type DateTimeGetDate =
    unsafe extern "C" fn(Handle, Handle, Handle, *mut i16, *mut u8, *mut u8) -> Sword;
type DateTimeGetTime =
    unsafe extern "C" fn(Handle, Handle, Handle, *mut u8, *mut u8, *mut u8, *mut u32) -> Sword;
type DateTimeGetOffset = unsafe extern "C" fn(Handle, Handle, Handle, *mut i8, *mut i8) -> Sword;

/// The resolved OCI entry points. The function pointers stay valid because `library` is kept alive next to them
/// and is dropped last.
pub struct Client {
    pub(super) env_nls_create: EnvNlsCreate,
    pub(super) handle_alloc: HandleAlloc,
    pub(super) handle_free: HandleFree,
    pub(super) server_attach: ServerAttach,
    pub(super) server_detach: ServerDetach,
    pub(super) attr_set: AttrSet,
    pub(super) session_begin: SessionBegin,
    pub(super) session_end: SessionEnd,
    pub(super) error_get: ErrorGet,
    pub(super) out_attach: OutAttach,
    pub(super) out_receive: OutReceive,
    pub(super) out_lwm_set: OutLwmSet,
    pub(super) out_detach: OutDetach,
    pub(super) chunk_receive: ChunkReceive,
    pub(super) row_column_info_get: RowColumnInfoGet,
    pub(super) header_get: HeaderGet,
    pub(super) ddl_info_get: DdlInfoGet,
    pub(super) date_time_get_date: DateTimeGetDate,
    pub(super) date_time_get_time: DateTimeGetTime,
    pub(super) date_time_get_offset: DateTimeGetOffset,
    _library: Library,
}

impl Client {
    pub fn load(path: Option<&str>) -> Result<Self, StreamError> {
        let path = path.unwrap_or(default_library_name());
        // SAFETY: loading a shared library runs its initializers; the path is operator configuration.
        let library = unsafe { Library::new(path) }.map_err(|error| {
            StreamError::Library(describe_load_failure(path, &error.to_string()))
        })?;
        macro_rules! symbol {
            ($name:literal) => {
                // SAFETY: the declared signature is the documented OCI prototype (ocixstream.h and oci.h).
                *unsafe { library.get(concat!($name, "\0").as_bytes()) }.map_err(|error| {
                    StreamError::Library(format!("{path} has no symbol {}: {error}", $name))
                })?
            };
        }
        Ok(Self {
            env_nls_create: symbol!("OCIEnvNlsCreate"),
            handle_alloc: symbol!("OCIHandleAlloc"),
            handle_free: symbol!("OCIHandleFree"),
            server_attach: symbol!("OCIServerAttach"),
            server_detach: symbol!("OCIServerDetach"),
            attr_set: symbol!("OCIAttrSet"),
            session_begin: symbol!("OCISessionBegin"),
            session_end: symbol!("OCISessionEnd"),
            error_get: symbol!("OCIErrorGet"),
            out_attach: symbol!("OCIXStreamOutAttach"),
            out_receive: symbol!("OCIXStreamOutLCRReceive"),
            out_lwm_set: symbol!("OCIXStreamOutProcessedLWMSet"),
            out_detach: symbol!("OCIXStreamOutDetach"),
            chunk_receive: symbol!("OCIXStreamOutChunkReceive"),
            row_column_info_get: symbol!("OCILCRRowColumnInfoGet"),
            header_get: symbol!("OCILCRHeaderGet"),
            ddl_info_get: symbol!("OCILCRDDLInfoGet"),
            date_time_get_date: symbol!("OCIDateTimeGetDate"),
            date_time_get_time: symbol!("OCIDateTimeGetTime"),
            date_time_get_offset: symbol!("OCIDateTimeGetTimeZoneOffset"),
            _library: library,
        })
    }
}

fn default_library_name() -> &'static str {
    if cfg!(target_os = "macos") {
        "libclntsh.dylib"
    } else if cfg!(target_os = "windows") {
        "oci.dll"
    } else {
        "libclntsh.so"
    }
}

fn describe_load_failure(path: &str, reason: &str) -> String {
    let hint = if reason.contains("libaio") {
        " (libclntsh needs the libaio package on the host)"
    } else {
        ""
    };
    format!("cannot load {path}: {reason}{hint}")
}
