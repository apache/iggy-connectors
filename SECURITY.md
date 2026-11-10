# Security Policy

Apache Iggy Connectors are part of Apache Iggy. The
[security policy of Apache Iggy](https://github.com/apache/iggy/blob/master/SECURITY.md) applies to them. That
policy follows the [security process](https://www.apache.org/security/) of the Apache Software Foundation.
Please read it before you report anything that you think is a security issue.

## Reporting a Vulnerability

**Do not report security vulnerabilities through public channels.** That means no GitHub issues, pull
requests or discussions, no Discord, and no social media. Public disclosure before a fix is available
puts users at risk.

Send reports to **[security@apache.org](mailto:security@apache.org)**.

Iggy does not have its own project security list, so `security@apache.org` is the correct address. The ASF
Security Team forwards your report to the private list of the Iggy PMC and confirms to you that it did so.

When you report, please:

- send one plain-text, unencrypted email per vulnerability
- describe the issue in the body, and do not attach images, video, HTML or PDF
- include the affected version or commit, and the component: the runtime, the SDK, or the name of the sink
  or source
- include the configuration that is necessary to hit it
- include the steps to reproduce it and your assessment of the impact

The policy of Apache Iggy describes
[what happens next](https://github.com/apache/iggy/blob/master/SECURITY.md#what-happens-next) after a report.
It also lists the reports that are [out of scope](https://github.com/apache/iggy/blob/master/SECURITY.md#out-of-scope)
for Iggy.

## Connectors

- A plugin runs inside the runtime process, with the privileges of the runtime. Load only plugins that you
  trust.
- The HTTP API of the runtime returns connector configurations with their credentials, and it accepts writes.
  The [runtime documentation](runtime/README.md#http-api) describes how to protect it.

## References

- [ASF Security Team](https://www.apache.org/security/)
- [Reporting security issues in ASF code](https://security.apache.org/report-code/)
- [Vulnerability handling for committers](https://www.apache.org/security/committers.html)
