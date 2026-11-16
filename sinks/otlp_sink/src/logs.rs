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

use iggy_connector_sdk::Error;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};

const TRACE_ID_BYTES: usize = 16;
const SPAN_ID_BYTES: usize = 8;

/// The log records in every resource entry of `request`.
pub(crate) fn log_record_count(request: &ExportLogsServiceRequest) -> u64 {
    request
        .resource_logs
        .iter()
        .flat_map(|resource| &resource.scope_logs)
        .map(|scope| scope.log_records.len() as u64)
        .sum()
}

/// Check that every trace ID and span ID is absent or has the length OTLP defines.
///
/// The decode accepts an ID of any length, because the field is plain `bytes` to protobuf. The
/// Collector checks the length, and rejects the whole request for one ID that fails.
pub(crate) fn check_id_lengths(request: &ExportLogsServiceRequest) -> Result<(), Error> {
    let log_records = request
        .resource_logs
        .iter()
        .flat_map(|resource| &resource.scope_logs)
        .flat_map(|scope| &scope.log_records);
    for log_record in log_records {
        check_id_length("trace_id", &log_record.trace_id, TRACE_ID_BYTES)?;
        check_id_length("span_id", &log_record.span_id, SPAN_ID_BYTES)?;
    }
    Ok(())
}

/// Log records the receiver accepted the request but dropped, with its reason.
pub(crate) fn rejected_log_records(response: &ExportLogsServiceResponse) -> Option<(i64, &str)> {
    response
        .partial_success
        .as_ref()
        .filter(|partial| partial.rejected_log_records > 0)
        .map(|partial| (partial.rejected_log_records, partial.error_message.as_str()))
}

fn check_id_length(field: &str, id: &[u8], expected: usize) -> Result<(), Error> {
    if id.is_empty() || id.len() == expected {
        return Ok(());
    }
    Err(Error::InvalidRecordValue(format!(
        "a log record has a {field} of {} bytes, not {expected}",
        id.len()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsPartialSuccess;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};

    fn resource(records_per_scope: &[usize]) -> ResourceLogs {
        ResourceLogs {
            scope_logs: records_per_scope
                .iter()
                .map(|records| ScopeLogs {
                    log_records: vec![LogRecord::default(); *records],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    /// Two resources, with `log_record` as the last record of the second one, so a check that
    /// stops at the first resource, scope or record does not reach it.
    fn request_ending_with(log_record: LogRecord) -> ExportLogsServiceRequest {
        let mut last = resource(&[1, 1]);
        last.scope_logs[1].log_records.push(log_record);
        ExportLogsServiceRequest {
            resource_logs: vec![resource(&[1]), last],
        }
    }

    #[test]
    fn given_request_should_count_every_record_across_resources_and_scopes() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![resource(&[2, 1]), resource(&[3])],
        };

        assert_eq!(log_record_count(&request), 6);
    }

    #[test]
    fn given_ids_that_are_absent_or_of_the_defined_length_should_pass() {
        let request = request_ending_with(LogRecord {
            trace_id: vec![1; 16],
            span_id: vec![1; 8],
            ..Default::default()
        });

        assert_eq!(check_id_lengths(&request), Ok(()));
    }

    #[test]
    fn given_trace_id_of_another_length_should_fail() {
        // A trace ID written as its 32 hex characters, not as 16 bytes.
        let request = request_ending_with(LogRecord {
            trace_id: b"5b8efff798038103d269b633813fc60c".to_vec(),
            ..Default::default()
        });

        assert_eq!(
            check_id_lengths(&request),
            Err(Error::InvalidRecordValue(
                "a log record has a trace_id of 32 bytes, not 16".to_owned()
            ))
        );
    }

    #[test]
    fn given_span_id_of_another_length_should_fail() {
        let request = request_ending_with(LogRecord {
            trace_id: vec![1; 16],
            span_id: vec![1; 7],
            ..Default::default()
        });

        assert_eq!(
            check_id_lengths(&request),
            Err(Error::InvalidRecordValue(
                "a log record has a span_id of 7 bytes, not 8".to_owned()
            ))
        );
    }

    #[test]
    fn given_partial_success_without_rejections_should_not_report_rejected() {
        let response = ExportLogsServiceResponse {
            partial_success: Some(ExportLogsPartialSuccess {
                rejected_log_records: 0,
                error_message: "warning only".to_owned(),
            }),
        };

        assert!(rejected_log_records(&response).is_none());
    }

    #[test]
    fn given_partial_success_with_rejections_should_report_count_and_reason() {
        let response = ExportLogsServiceResponse {
            partial_success: Some(ExportLogsPartialSuccess {
                rejected_log_records: 2,
                error_message: "too old".to_owned(),
            }),
        };

        assert_eq!(rejected_log_records(&response), Some((2, "too old")));
    }
}
