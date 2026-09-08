# Security Policy

## Reporting

Please report security issues privately through GitHub's security reporting facilities when available. Do not include credentials, production message payloads, or other sensitive data in public issues.

## Current security boundaries

RelayGrid v0.1 is an in-memory runtime library. It does not authenticate producers or consumers, encrypt transport, persist payloads, or provide multi-tenant isolation.

Callers are responsible for validating message payloads before handing them to application handlers. Exception messages copied to a dead-letter record can contain handler-provided text; applications should avoid putting secrets in exception messages or should sanitize them in their dead-letter implementation.

A dead-letter sink failure faults the runtime rather than dropping work silently.
