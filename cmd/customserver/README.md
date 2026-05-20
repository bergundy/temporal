# customserver

A near-copy of the upstream `temporal-server` binary, with two extension
points overridden:

- **Authorizer**: `cmd/customserver/nexusauthz.NewAuthorizer`, which rewrites
  the `claims` Nexus header on outgoing `ScheduleNexusOperation` commands
  with the server-attested caller identity, and enforces an
  endpoint/service/operation policy on inbound Nexus dispatches.
- **ClaimMapper**: `cmd/customserver/nexusauthz.NewClaimMapper`, which wraps
  the upstream `DefaultJWTClaimMapper` and stashes the raw `permissions`
  array claim in `Claims.Extensions` so the authorizer can serialize it into
  the propagated header.

Everything else — config loading, service selection, audience getter, logger
setup — mirrors `cmd/server/main.go` verbatim.

## Build

```
go build -o customserver ./cmd/customserver
```

## Run

```
NEXUS_AUTHZ_POLICY_FILE=$PWD/policy.yaml \
  ./customserver start --service=frontend,history,matching,worker --config $PWD/config
```

`NEXUS_AUTHZ_POLICY_FILE` defaults to `./policy.yaml` if unset. The config
directory should hold a `development-jwt.yaml`-style configuration so the
upstream JWT key provider is wired up correctly.
