# Security Policy

Do not report vulnerabilities, credentials, model-service tokens, private game histories, production logs, or exploit details in a public issue.

## Private reporting

- Email: masterai918@gmail.com
- Subject: `Security report: cfr-poker-ai-masterai`

Include the affected revision and component, impact, reproduction steps, and a minimal proof of concept. Remove personal data and unrelated confidential information.

## Scope

Only the latest revision of the default branch is evaluated unless the maintainer states otherwise. Acknowledgement and remediation times are not guaranteed.

## Deployment responsibility

Before exposing any model or service to a network, review authentication, authorization, input validation, protocol handling, deserialization, subprocess execution, Redis access, model loading, secrets, logs, dependencies, and denial-of-service limits. Use isolated test data and conduct an independent security assessment.
