# Security Policy

## Reporting a Vulnerability

Report SigV4, credential, authentication, authorization, or private-bucket issues privately. Do not open a public issue with exploit details.

Use a [private GitHub security advisory](https://github.com/TalkingComputers/holys3/security/advisories/new) for this repository. If that is unavailable, contact a maintainer privately and share only a short non-sensitive summary until a private channel is established.

Include:

- affected command or crate;
- AWS region and S3 operation involved, if relevant;
- whether credentials, signatures, headers, or object data may be exposed;
- minimal reproduction steps without real secrets.

Never include AWS access keys, secret keys, session tokens, or private object contents in a report.

## Release Integrity

Every release archive has a SHA-256 checksum and a GitHub build-provenance attestation. Verify a downloaded archive with:

```console
$ gh attestation verify <downloaded-archive> -R TalkingComputers/holys3
```
