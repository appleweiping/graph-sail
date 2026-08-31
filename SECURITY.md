# Security policy

## Supported versions

Security fixes are applied to the latest release on the default branch.

## Reporting a vulnerability

Please use GitHub's private vulnerability reporting feature instead of opening a public issue.
Include the affected version, a minimal reproduction, impact, and any suggested mitigation. You
should receive an acknowledgement within seven days.

## Trust boundaries

Graph Sail parses untrusted JSON and emits JSON, DOT, and HTML. It does not execute graph nodes,
download models, open remote URLs, invoke Graphviz, or contact external services. HTML text and DOT
labels are escaped before rendering. Applications embedding reports should still apply their own
content-security policy.

A generated plan is an estimate, not authorization to allocate hardware or move sensitive data.
Callers must enforce their own device-access, tenancy, and data-residency controls.
