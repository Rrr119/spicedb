# Relationship versioning

SpiceDB can store relationships in up to three independent logical layouts:
`v1`, `v2`, and `v3`. `v1` is the legacy layout and is the default. It keeps the
original namespace names and is fully compatible with existing data and clients.
Later versions use an internal, hidden namespace prefix; the DataLayer translates
between those names and the public API, so callers continue to use the same
resource types and relationship strings.

The `sdb_rv` namespace prefix is reserved for this mechanism and
must not be used by application schemas.

Relationship versions are independent of schema definition versions. A check at
a revision always uses the schema stored at that revision. The relationship
version selects which relationship layout is read or written.

## Selecting a version

Set a server default:

```text
--experimental-relationship-version=v1
--experimental-relationship-dual-write=v2
```

The read version is always written. Each version in
`--experimental-relationship-dual-write` is written as well. For example, the
configuration above keeps old clients on `v1` while also producing `v2` data.

Existing API requests can select a version without changing their protobuf
messages by sending gRPC metadata:

```text
io.spicedb.relationship-version: v2
```

The `x-spicedb-relationship-version` header is accepted as an alias. Requests
without either value use the configured default, preserving the behavior of
existing clients.

In-process callers can use
`datalayer.ContextWithRelationshipVersion(ctx, version)`.

## Migration

`datalayer.MigrateRelationships` copies relationships from a source version to a
target version inside one datastore transaction. The target is made an exact
mirror of the source for the selected filter, so stale target relationships are
removed. The source is not modified.
Because the copy and an optional schema write share the transaction, an
interruption before commit leaves neither partial relationships nor a partial
schema.

```go
revision, result, err := datalayer.MigrateRelationships(ctx, dl, datalayer.RelationshipMigration{
    Source: datalayer.RelationshipVersionV1,
    Target: datalayer.RelationshipVersionV2,
})
```

A typical rollout is:

1. Configure reads on `v1` and dual-write `v1,v2`.
2. Run the `v1 -> v2` migration.
3. Verify checks with `io.spicedb.relationship-version: v2`.
4. Move the read default to `v2`, keeping `v1` in the dual-write set for
   rollback.
5. Repeat for `v3` when ready.

More than one schema version can remain readable at the same time. Checks made
at a schema revision continue to use that revision's schema, regardless of which
relationship version handles the tuples.

## Rollback

`datalayer.RollbackRelationshipMigration` deletes only relationships present in
the target that are also present in the source. The source version is never
deleted, and target-only relationships are preserved.

```go
revision, result, err := datalayer.RollbackRelationshipMigration(ctx, dl, datalayer.RelationshipRollback{
    Source: datalayer.RelationshipVersionV1,
    Target: datalayer.RelationshipVersionV2,
})
```

If the migration changed the schema, pass the previous schema in `Schema` to
restore it in the same rollback transaction.

For discretionary deletes and imports, use the normal relationship APIs. The
DataLayer routes reads by version and applies deletes or bulk imports to every
configured write version.
