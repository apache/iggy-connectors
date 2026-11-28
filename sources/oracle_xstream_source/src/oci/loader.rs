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

/// Declares the OCI entry points once: the function-pointer fields of [`Client`] and their lookup by symbol name.
/// The signatures are the documented OCI prototypes (ocixstream.h and oci.h).
macro_rules! oci_functions {
    ($($field:ident: $symbol:literal, $signature:ty;)*) => {
        /// The resolved OCI entry points. The function pointers stay valid because `library` is kept alive next to
        /// them and is dropped last.
        pub struct Client {
            $(pub(super) $field: $signature,)*
            _library: Library,
        }

        impl Client {
            pub fn load(path: Option<&str>) -> Result<Self, StreamError> {
                let path = path.unwrap_or(default_library_name());
                // SAFETY: loading a shared library runs its initializers; the path is operator configuration.
                let library = unsafe { Library::new(path) }.map_err(|error| {
                    StreamError::Library(describe_load_failure(path, &error.to_string()))
                })?;
                Ok(Self {
                    $($field: symbol(&library, path, concat!($symbol, "\0"))?,)*
                    _library: library,
                })
            }
        }
    };
}

oci_functions! {
    env_nls_create: "OCIEnvNlsCreate", unsafe extern "C" fn(
        *mut Handle, u32, Handle, Handle, Handle, Handle, usize, *mut Handle, u16, u16) -> Sword;
    handle_alloc: "OCIHandleAlloc", unsafe extern "C" fn(Handle, *mut Handle, u32, usize, *mut Handle) -> Sword;
    handle_free: "OCIHandleFree", unsafe extern "C" fn(Handle, u32) -> Sword;
    server_attach: "OCIServerAttach", unsafe extern "C" fn(Handle, Handle, *const u8, i32, u32) -> Sword;
    server_detach: "OCIServerDetach", unsafe extern "C" fn(Handle, Handle, u32) -> Sword;
    attr_set: "OCIAttrSet", unsafe extern "C" fn(Handle, u32, Handle, u32, u32, Handle) -> Sword;
    session_begin: "OCISessionBegin", unsafe extern "C" fn(Handle, Handle, Handle, u32, u32) -> Sword;
    session_end: "OCISessionEnd", unsafe extern "C" fn(Handle, Handle, Handle, u32) -> Sword;
    error_get: "OCIErrorGet", unsafe extern "C" fn(Handle, u32, *mut u8, *mut i32, *mut u8, u32, u32) -> Sword;
    out_attach: "OCIXStreamOutAttach", unsafe extern "C" fn(Handle, Handle, *const u8, u16, *const u8, u16, u32) -> Sword;
    out_receive: "OCIXStreamOutLCRReceive", unsafe extern "C" fn(
        Handle, Handle, *mut Handle, *mut u8, *mut u64, *mut u8, *mut u16, u32) -> Sword;
    out_lwm_set: "OCIXStreamOutProcessedLWMSet", unsafe extern "C" fn(Handle, Handle, *const u8, u16, u32) -> Sword;
    out_detach: "OCIXStreamOutDetach", unsafe extern "C" fn(Handle, Handle, u32) -> Sword;
    chunk_receive: "OCIXStreamOutChunkReceive", unsafe extern "C" fn(
        Handle, Handle, *mut Text, *mut u16, *mut u16, *mut u64, *mut u16, *mut u32, *mut Text, *mut u64, u32) -> Sword;
    row_column_info_get: "OCILCRRowColumnInfoGet", unsafe extern "C" fn(
        Handle, Handle, u16, *mut u16, *mut Text, *mut u16, *mut u16, *mut Handle, *mut i16, *mut u16, *mut u8,
        *mut u64, *mut u16, Handle, u16, u32) -> Sword;
    header_get: "OCILCRHeaderGet", unsafe extern "C" fn(
        Handle, Handle, *mut Text, *mut u16, *mut Text, *mut u16, *mut Text, *mut u16, *mut Text, *mut u16,
        *mut Text, *mut u16, *mut Text, *mut u16, Handle, *mut u16, *mut u16, *mut Text, *mut u16, *mut u64,
        Handle, u32) -> Sword;
    ddl_info_get: "OCILCRDDLInfoGet", unsafe extern "C" fn(
        Handle, Handle, *mut Text, *mut u16, *mut Text, *mut u32, *mut Text, *mut u16, *mut Text, *mut u16,
        *mut Text, *mut u16, *mut Text, *mut u16, *mut u64, Handle, u32) -> Sword;
    date_time_get_date: "OCIDateTimeGetDate", unsafe extern "C" fn(
        Handle, Handle, Handle, *mut i16, *mut u8, *mut u8) -> Sword;
    date_time_get_time: "OCIDateTimeGetTime", unsafe extern "C" fn(
        Handle, Handle, Handle, *mut u8, *mut u8, *mut u8, *mut u32) -> Sword;
    date_time_get_offset: "OCIDateTimeGetTimeZoneOffset", unsafe extern "C" fn(
        Handle, Handle, Handle, *mut i8, *mut i8) -> Sword;
}

/// `name` ends with a NUL byte, as `libloading` expects for a C symbol.
fn symbol<T: Copy>(library: &Library, path: &str, name: &str) -> Result<T, StreamError> {
    // SAFETY: `T` is the documented OCI prototype of `name`, fixed by the field type in `oci_functions!`.
    unsafe { library.get::<T>(name.as_bytes()) }
        .map(|symbol| *symbol)
        .map_err(|error| {
            StreamError::Library(format!(
                "{path} has no symbol {}: {error}",
                name.trim_end_matches('\0')
            ))
        })
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
