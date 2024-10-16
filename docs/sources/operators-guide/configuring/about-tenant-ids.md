---
title: "About Grafana Mimir tenant IDs"
menuTitle: "About tenant IDs"
description: "Learn about tenant ID restrictions."
weight: 10
---

# About Grafana Mimir tenant IDs

Within a Grafana Mimir cluster, the tenant ID is the unique identifier of a tenant.
For information about how Grafana Mimir components use tenant IDs, refer to [Authentication and authorization]({{< relref "../securing/authentication-and-authorization.md" >}}).

## Restrictions

Tenant IDs must be less-than or equal-to 150 bytes or characters in length and can only include the following supported characters:

- Alphanumeric characters
  - `0-9`
  - `a-z`
  - `A-Z`
- Special characters
  - Exclamation point (`!`)
  - Hyphen (`-`)
  - Underscore (`_`)
  - Single period (`.`)
  - Asterisk (`*`)
  - Single quote (`'`)
  - Open parenthesis (`(`)
  - Close parenthesis (`)`)

> **Note:** For security reasons, `.` and `..` are not valid tenant IDs.

All other characters, including slashes and whitespace, are not supported.
