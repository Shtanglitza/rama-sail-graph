# Security Policy

## Reporting Vulnerabilities

If you discover a security vulnerability, please report it responsibly:

1. **Do not** open a public GitHub issue
2. Use [GitHub Security Advisories](https://github.com/Shtanglitza/rama-sail-graph/security/advisories/new) to report privately
3. Include steps to reproduce, impact assessment, and any suggested fixes

We will acknowledge receipt within 48 hours and provide a timeline for resolution.

## Scope

The following are in scope for security reports:

- SPARQL injection or query manipulation
- Resource exhaustion / denial of service via crafted queries
- Authentication bypass (when auth is configured)
- Information disclosure via error messages or metrics
- Unsafe deserialization or code execution

## Known Limitations

The SPARQL HTTP endpoint (`rama-sail.server.sparql`) is designed for **internal/trusted network** use:

- No built-in authentication or authorization
- CORS allows all origins by default
- No request body size limits
- No rate limiting

These are documented design choices for the current version. If you deploy this endpoint on untrusted networks, you should add a reverse proxy with authentication, rate limiting, and request size restrictions.

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |
