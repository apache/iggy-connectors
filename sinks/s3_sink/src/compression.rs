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
use std::io::Write;

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
            Self::Gzip => {
                let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(&data).and_then(|()| encoder.finish())
            }
            Self::Zstd => zstd::stream::encode_all(data.as_slice(), 0),
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

#[cfg(test)]
mod tests {
    use std::io::Read;

    use flate2::read::GzDecoder;

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
    fn given_gzip_when_encoding_should_round_trip_empty_and_binary_data() {
        for data in [Vec::new(), (0..=255).cycle().take(128 * 1024).collect()] {
            let encoded = Compression::Gzip.encode(data.clone()).unwrap();
            let mut decoded = Vec::new();
            GzDecoder::new(encoded.as_slice())
                .read_to_end(&mut decoded)
                .unwrap();
            assert_eq!(decoded, data);
        }
    }

    #[test]
    fn given_zstd_when_encoding_should_round_trip_empty_and_binary_data() {
        for data in [Vec::new(), (0..=255).cycle().take(128 * 1024).collect()] {
            let encoded = Compression::Zstd.encode(data.clone()).unwrap();
            let decoded = zstd::stream::decode_all(encoded.as_slice()).unwrap();
            assert_eq!(decoded, data);
        }
    }
}
