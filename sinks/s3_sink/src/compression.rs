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

use std::fmt;
use std::io::{self, Write};

use flate2::write::GzEncoder;
use iggy_connector_sdk::Error;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    #[default]
    None,
    Gzip,
    Zstd,
}

impl Compression {
    pub(crate) fn encode(self, data: Vec<u8>) -> Result<Vec<u8>, Error> {
        let result = match self {
            Self::None => return Ok(data),
            Self::Gzip => encode_gzip(&data),
            Self::Zstd => encode_zstd(&data),
        };
        result.map_err(|error| {
            Error::CannotStoreData(format!(
                "Failed to compress S3 object using {self}: {error}"
            ))
        })
    }

    pub(crate) fn file_suffix(self) -> &'static str {
        match self {
            Self::None => "",
            Self::Gzip => ".gz",
            Self::Zstd => ".zst",
        }
    }

    pub(crate) fn content_encoding(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Gzip => Some("gzip"),
            Self::Zstd => Some("zstd"),
        }
    }

    #[cfg(test)]
    pub(crate) fn decode(self, data: &[u8]) -> Vec<u8> {
        match self {
            Self::None => data.to_vec(),
            Self::Gzip => {
                let mut decoded = Vec::new();
                io::copy(&mut flate2::read::GzDecoder::new(data), &mut decoded).unwrap();
                decoded
            }
            Self::Zstd => zstd::stream::decode_all(data).unwrap(),
        }
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => formatter.write_str("none"),
            Self::Gzip => formatter.write_str("gzip"),
            Self::Zstd => formatter.write_str("zstd"),
        }
    }
}

fn encode_gzip(data: &[u8]) -> io::Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(data)?;
    encoder.finish()
}

// libzstd writes no checksum unless asked, while every gzip member carries a CRC32.
// The pledged size also records the decompressed size in the frame header.
fn encode_zstd(data: &[u8]) -> io::Result<Vec<u8>> {
    let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 0)?;
    encoder.include_checksum(true)?;
    encoder.set_pledged_src_size(Some(data.len() as u64))?;
    encoder.write_all(data)?;
    encoder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_no_compression_when_encoding_should_preserve_bytes_and_allocation() {
        let data = b"\x00\xffraw\nbytes".to_vec();
        let original_ptr = data.as_ptr();
        let encoded = Compression::None.encode(data).unwrap();
        assert_eq!(encoded, b"\x00\xffraw\nbytes");
        assert_eq!(encoded.as_ptr(), original_ptr);
    }

    #[test]
    fn given_codec_when_encoding_should_round_trip_empty_and_binary_data() {
        let binary: Vec<u8> = (0..=255).cycle().take(128 * 1024).collect();
        for compression in [Compression::Gzip, Compression::Zstd] {
            for data in [Vec::new(), binary.clone()] {
                let encoded = compression.encode(data.clone()).unwrap();
                assert_eq!(compression.decode(&encoded), data);
            }
        }
    }

    #[test]
    fn given_zstd_when_encoding_should_record_content_size_and_checksum() {
        let encoded = Compression::Zstd.encode(vec![b'x'; 4096]).unwrap();
        assert_eq!(
            zstd::zstd_safe::get_frame_content_size(&encoded).unwrap(),
            Some(4096)
        );
        // RFC 8878: the frame header descriptor follows the 4-byte magic number,
        // and its bit 2 is Content_Checksum_flag.
        assert_ne!(encoded[4] & 0b100, 0);
    }
}
