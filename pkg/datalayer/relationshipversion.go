package datalayer

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/grpc/metadata"

	"github.com/authzed/ctxkey"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

// RelationshipVersion identifies a relationship storage version. Versions are
// independent logical relationship stores backed by the same datastore.
//
// V1 is the legacy store and uses the original namespace names. Later versions
// use an internal namespace prefix, which acts as the indirection layer between
// the public API and version-specific storage. The prefix is never exposed by
// the DataLayer.
type RelationshipVersion string

const (
	// RelationshipVersionV1 is the legacy relationship storage layout.
	RelationshipVersionV1 RelationshipVersion = "v1"

	// RelationshipVersionV2 is the first versioned relationship storage layout.
	RelationshipVersionV2 RelationshipVersion = "v2"

	// RelationshipVersionV3 is the second versioned relationship storage layout.
	RelationshipVersionV3 RelationshipVersion = "v3"
)

const (
	// RelationshipVersionMetadataKey is the gRPC metadata key used to select a
	// relationship version for an existing API request.
	RelationshipVersionMetadataKey = "io.spicedb.relationship-version"

	// RelationshipVersionHTTPHeader is the equivalent HTTP header. It is accepted
	// as an alias by the gRPC interceptors for gateway compatibility.
	RelationshipVersionHTTPHeader = "x-spicedb-relationship-version"

	relationshipVersionNamespacePrefix = "sdb_rv"
)

var supportedRelationshipVersions = []RelationshipVersion{
	RelationshipVersionV1,
	RelationshipVersionV2,
	RelationshipVersionV3,
}

var relationshipVersionKey = ctxkey.NewBoxedWithDefault[RelationshipVersion]("")

// SupportedRelationshipVersions returns all relationship versions understood by
// the DataLayer.
func SupportedRelationshipVersions() []RelationshipVersion {
	return slices.Clone(supportedRelationshipVersions)
}

// ParseRelationshipVersion parses a relationship version. The empty string is
// accepted as an alias for V1 for backwards compatibility.
func ParseRelationshipVersion(value string) (RelationshipVersion, error) {
	version := RelationshipVersion(strings.ToLower(strings.TrimSpace(value)))
	switch version {
	case "":
		return RelationshipVersionV1, nil
	case RelationshipVersionV1, RelationshipVersionV2, RelationshipVersionV3:
		return version, nil
	default:
		return "", fmt.Errorf("invalid relationship version %q, must be one of: v1, v2, v3", value)
	}
}

// IsValid reports whether the version is known.
func (v RelationshipVersion) IsValid() bool {
	return slices.Contains(supportedRelationshipVersions, v)
}

// String returns the canonical version name.
func (v RelationshipVersion) String() string {
	if v == "" {
		return string(RelationshipVersionV1)
	}
	return string(v)
}

func normalizeRelationshipVersion(version RelationshipVersion) RelationshipVersion {
	if version == "" {
		return RelationshipVersionV1
	}
	return version
}

// ContextWithRelationshipVersion selects the relationship version for reads and
// writes performed with the returned context. Existing callers that do not set
// a version continue to use V1 (or the DataLayer's configured default).
func ContextWithRelationshipVersion(ctx context.Context, version RelationshipVersion) context.Context {
	normalized := normalizeRelationshipVersion(version)
	ctx = relationshipVersionKey.SetBox(ctx)
	relationshipVersionKey.Set(ctx, normalized)
	return ctx
}

// RelationshipVersionFromContext returns the explicitly selected relationship
// version, if any.
func RelationshipVersionFromContext(ctx context.Context) (RelationshipVersion, bool) {
	version := relationshipVersionKey.Value(ctx)
	if version == "" {
		return RelationshipVersionV1, false
	}
	return normalizeRelationshipVersion(version), true
}

func relationshipVersionFromMetadata(ctx context.Context) (RelationshipVersion, bool, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false, nil
	}

	for _, key := range []string{RelationshipVersionMetadataKey, RelationshipVersionHTTPHeader} {
		values := md.Get(key)
		if len(values) == 0 {
			continue
		}
		if len(values) > 1 {
			return "", false, fmt.Errorf("multiple %s metadata values are not allowed", key)
		}
		version, err := ParseRelationshipVersion(values[0])
		if err != nil {
			return "", false, err
		}
		return version, true, nil
	}

	return "", false, nil
}

type relationshipVersionConfig struct {
	read  RelationshipVersion
	write []RelationshipVersion
}

func newRelationshipVersionConfig(read RelationshipVersion, dualWrite ...RelationshipVersion) relationshipVersionConfig {
	read = normalizeRelationshipVersion(read)
	write := []RelationshipVersion{read}
	for _, version := range dualWrite {
		version = normalizeRelationshipVersion(version)
		if !slices.Contains(write, version) {
			write = append(write, version)
		}
	}
	return relationshipVersionConfig{read: read, write: write}
}

func (c relationshipVersionConfig) readVersion(ctx context.Context) (RelationshipVersion, error) {
	version, ok := RelationshipVersionFromContext(ctx)
	if !ok {
		version = c.read
	}
	version = normalizeRelationshipVersion(version)
	if !version.IsValid() {
		return "", fmt.Errorf("invalid relationship version %q", version)
	}
	return version, nil
}

func (c relationshipVersionConfig) writeVersions(ctx context.Context) ([]RelationshipVersion, error) {
	versions := slices.Clone(c.write)
	if version, ok := RelationshipVersionFromContext(ctx); ok {
		if !version.IsValid() {
			return nil, fmt.Errorf("invalid relationship version %q", version)
		}
		if !slices.Contains(versions, version) {
			if len(versions) <= 1 {
				versions = []RelationshipVersion{version}
			} else {
				versions = append(versions, version)
			}
		}
	}
	if len(versions) == 0 {
		versions = []RelationshipVersion{RelationshipVersionV1}
	}
	for _, version := range versions {
		if !version.IsValid() {
			return nil, fmt.Errorf("invalid relationship version %q", version)
		}
	}
	return versions, nil
}

// WithRelationshipVersioning configures the default read version and the set of
// versions that receive every write. The read version is always dual-written.
// Passing no dual-write versions keeps the read version as the only write
// target. Existing callers that omit this option continue to use V1 only.
func WithRelationshipVersioning(read RelationshipVersion, dualWrite ...RelationshipVersion) DataLayerOption {
	return func(d *defaultDataLayer) {
		d.relationshipVersions = newRelationshipVersionConfig(read, dualWrite...)
	}
}

// WithRelationshipVersion is an alias for WithRelationshipVersioning.
func WithRelationshipVersion(read RelationshipVersion, dualWrite ...RelationshipVersion) DataLayerOption {
	return WithRelationshipVersioning(read, dualWrite...)
}

func (v RelationshipVersion) storagePrefix() string {
	v = normalizeRelationshipVersion(v)
	if v == RelationshipVersionV1 {
		return ""
	}
	return relationshipVersionNamespacePrefix + strings.TrimPrefix(string(v), "v") + "_"
}

func (v RelationshipVersion) storageNamespace(namespace string) string {
	if namespace == "" {
		return ""
	}
	return v.storagePrefix() + namespace
}

func (v RelationshipVersion) logicalNamespace(namespace string) (string, bool) {
	prefix := v.storagePrefix()
	if prefix == "" {
		return namespace, !strings.HasPrefix(namespace, relationshipVersionNamespacePrefix)
	}
	logical, ok := strings.CutPrefix(namespace, prefix)
	return logical, ok
}

func (v RelationshipVersion) toStorageRelationship(relationship tuple.Relationship) tuple.Relationship {
	relationship.Resource.ObjectType = v.storageNamespace(relationship.Resource.ObjectType)
	relationship.Subject.ObjectType = v.storageNamespace(relationship.Subject.ObjectType)
	return relationship
}

func (v RelationshipVersion) fromStorageRelationship(relationship tuple.Relationship) (tuple.Relationship, bool) {
	resourceType, resourceOK := v.logicalNamespace(relationship.Resource.ObjectType)
	subjectType, subjectOK := v.logicalNamespace(relationship.Subject.ObjectType)
	if !resourceOK || !subjectOK {
		return tuple.Relationship{}, false
	}
	relationship.Resource.ObjectType = resourceType
	relationship.Subject.ObjectType = subjectType
	return relationship, true
}

func (v RelationshipVersion) translateRelationshipsFilter(filter datastore.RelationshipsFilter) datastore.RelationshipsFilter {
	filter.OptionalResourceType = v.storageNamespace(filter.OptionalResourceType)
	if len(filter.OptionalSubjectsSelectors) > 0 {
		selectors := slices.Clone(filter.OptionalSubjectsSelectors)
		for index := range selectors {
			selectors[index].OptionalSubjectType = v.storageNamespace(selectors[index].OptionalSubjectType)
		}
		filter.OptionalSubjectsSelectors = selectors
	}
	return filter
}

func (v RelationshipVersion) translateSubjectsFilter(filter datastore.SubjectsFilter) datastore.SubjectsFilter {
	filter.SubjectType = v.storageNamespace(filter.SubjectType)
	return filter
}

func (v RelationshipVersion) translateQueryOptions(opts ...options.QueryOptionsOption) []options.QueryOptionsOption {
	if v == RelationshipVersionV1 {
		return opts
	}

	queryOptions := options.NewQueryOptionsWithOptions(opts...)
	if queryOptions.After != nil {
		relationship := v.toStorageRelationship(*options.ToRelationship(queryOptions.After))
		queryOptions.After = options.ToCursor(relationship)
	}
	if queryOptions.BeforeOrEqual != nil {
		relationship := v.toStorageRelationship(*options.ToRelationship(queryOptions.BeforeOrEqual))
		queryOptions.BeforeOrEqual = options.ToCursor(relationship)
	}
	return []options.QueryOptionsOption{queryOptions.ToOption()}
}

func (v RelationshipVersion) translateReverseQueryOptions(opts ...options.ReverseQueryOptionsOption) []options.ReverseQueryOptionsOption {
	if v == RelationshipVersionV1 {
		return opts
	}

	queryOptions := options.NewReverseQueryOptionsWithOptions(opts...)
	if queryOptions.AfterForReverse != nil {
		relationship := v.toStorageRelationship(*options.ToRelationship(queryOptions.AfterForReverse))
		queryOptions.AfterForReverse = options.ToCursor(relationship)
	}
	if queryOptions.ResRelation != nil {
		resRelation := *queryOptions.ResRelation
		resRelation.Namespace = v.storageNamespace(resRelation.Namespace)
		queryOptions.ResRelation = &resRelation
	}
	return []options.ReverseQueryOptionsOption{queryOptions.ToOption()}
}

func (v RelationshipVersion) translateCursor(cursor options.Cursor) options.Cursor {
	if cursor == nil {
		return nil
	}
	return options.ToCursor(v.toStorageRelationship(*options.ToRelationship(cursor)))
}

func (v RelationshipVersion) logicalCursor(cursor options.Cursor) options.Cursor {
	if cursor == nil {
		return nil
	}
	relationship, ok := v.fromStorageRelationship(*options.ToRelationship(cursor))
	if !ok {
		return cursor
	}
	return options.ToCursor(relationship)
}

func relationshipIteratorForVersion(
	iter datastore.RelationshipIterator,
	version RelationshipVersion,
) datastore.RelationshipIterator {
	return func(yield func(tuple.Relationship, error) bool) {
		for relationship, err := range iter {
			if err != nil {
				yield(tuple.Relationship{}, err)
				return
			}
			logical, ok := version.fromStorageRelationship(relationship)
			if !ok {
				continue
			}
			if !yield(logical, nil) {
				return
			}
		}
	}
}

func relationshipChangesForVersion(
	ctx context.Context,
	changes <-chan datastore.RevisionChanges,
	errs <-chan error,
	version RelationshipVersion,
	content datastore.WatchContent,
) (<-chan datastore.RevisionChanges, <-chan error) {
	filteredChanges := make(chan datastore.RevisionChanges)
	filteredErrors := make(chan error, 1)

	go func() {
		defer close(filteredChanges)
		defer close(filteredErrors)

		for {
			select {
			case <-ctx.Done():
				filteredErrors <- ctx.Err()
				return
			case err, ok := <-errs:
				if !ok {
					errs = nil
					continue
				}
				if err != nil {
					filteredErrors <- err
					return
				}
			case change, ok := <-changes:
				if !ok {
					return
				}

				originalRelationshipChanges := change.RelationshipChanges
				change.RelationshipChanges = make([]tuple.RelationshipUpdate, 0, len(originalRelationshipChanges))
				for _, relationshipChange := range originalRelationshipChanges {
					logical, ok := version.fromStorageRelationship(relationshipChange.Relationship)
					if !ok {
						continue
					}
					relationshipChange.Relationship = logical
					change.RelationshipChanges = append(change.RelationshipChanges, relationshipChange)
				}

				hasNonRelationshipChanges := len(change.ChangedDefinitions) > 0 ||
					len(change.DeletedNamespaces) > 0 ||
					len(change.DeletedCaveats) > 0 ||
					change.StoredSchemaChanged ||
					change.IsCheckpoint ||
					len(change.Metadatas) > 0
				if len(originalRelationshipChanges) > 0 &&
					len(change.RelationshipChanges) == 0 &&
					!hasNonRelationshipChanges &&
					content&datastore.WatchRelationships != 0 {
					continue
				}
				select {
				case filteredChanges <- change:
				case <-ctx.Done():
					filteredErrors <- ctx.Err()
					return
				}
			}
		}
	}()

	return filteredChanges, filteredErrors
}

// RelationshipVersionedReader exposes explicit versioned relationship reads.
// The normal RevisionedReader methods select the version from the request
// context or the DataLayer default.
type RelationshipVersionedReader interface {
	QueryRelationshipsAtVersion(
		ctx context.Context,
		version RelationshipVersion,
		filter datastore.RelationshipsFilter,
		options ...options.QueryOptionsOption,
	) (datastore.RelationshipIterator, error)

	ReverseQueryRelationshipsAtVersion(
		ctx context.Context,
		version RelationshipVersion,
		subjectsFilter datastore.SubjectsFilter,
		options ...options.ReverseQueryOptionsOption,
	) (datastore.RelationshipIterator, error)
}

// QueryRelationshipsAtVersion reads relationships from an explicit relationship
// version when the reader supports relationship versioning.
func QueryRelationshipsAtVersion(
	ctx context.Context,
	reader RevisionedReader,
	version RelationshipVersion,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	versioned, ok := reader.(RelationshipVersionedReader)
	if !ok {
		return nil, fmt.Errorf("reader does not support relationship versioning")
	}
	return versioned.QueryRelationshipsAtVersion(ctx, version, filter, opts...)
}

// ReverseQueryRelationshipsAtVersion reads relationships from an explicit
// relationship version when the reader supports relationship versioning.
func ReverseQueryRelationshipsAtVersion(
	ctx context.Context,
	reader RevisionedReader,
	version RelationshipVersion,
	subjectsFilter datastore.SubjectsFilter,
	opts ...options.ReverseQueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	versioned, ok := reader.(RelationshipVersionedReader)
	if !ok {
		return nil, fmt.Errorf("reader does not support relationship versioning")
	}
	return versioned.ReverseQueryRelationshipsAtVersion(ctx, version, subjectsFilter, opts...)
}

// RelationshipMigration describes an atomic copy from one relationship version
// to another. The target is made an exact mirror of the source for the filter.
// The source version is never modified. If Schema is supplied, the schema and
// relationship copy are committed in the same datastore transaction.
type RelationshipMigration struct {
	Source RelationshipVersion
	Target RelationshipVersion
	Filter datastore.RelationshipsFilter

	// Schema, when non-nil, is written before the relationships are copied in the
	// same transaction.
	Schema *RelationshipSchemaMigration
}

// RelationshipSchemaMigration contains the schema write to perform as part of a
// relationship version migration.
type RelationshipSchemaMigration struct {
	Definitions   []datastore.SchemaDefinition
	SchemaText    string
	CaveatTypeSet *caveattypes.TypeSet
}

// RelationshipMigrationResult reports the outcome of a relationship migration.
type RelationshipMigrationResult struct {
	Copied        uint64
	RemovedStale  uint64
	SchemaWritten bool
}

// RelationshipRollback describes a rollback of one relationship version. When
// Source is supplied, only relationships present in both Source and Target are
// deleted from Target, preserving target-only data. Source itself is never
// modified.
type RelationshipRollback struct {
	Source RelationshipVersion
	Target RelationshipVersion
	Filter datastore.RelationshipsFilter

	// Schema, when non-nil, is restored before the target relationships are
	// deleted in the same transaction.
	Schema *RelationshipSchemaMigration
}

// RelationshipRollbackResult reports the outcome of a rollback.
type RelationshipRollbackResult struct {
	Deleted       uint64
	SchemaWritten bool
}

// RelationshipVersionMigrator exposes transactional migration and rollback
// operations on a read-write transaction.
type RelationshipVersionMigrator interface {
	MigrateRelationships(ctx context.Context, migration RelationshipMigration) (RelationshipMigrationResult, error)
	RollbackRelationshipMigration(ctx context.Context, rollback RelationshipRollback) (RelationshipRollbackResult, error)
}

// MigrateRelationships runs a relationship version migration in a single
// DataLayer transaction. An interruption or error rolls the whole migration
// back, so the source version remains complete.
func MigrateRelationships(ctx context.Context, dl DataLayer, migration RelationshipMigration) (datastore.Revision, RelationshipMigrationResult, error) {
	var result RelationshipMigrationResult
	revision, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		migrator, ok := rwt.(RelationshipVersionMigrator)
		if !ok {
			return fmt.Errorf("read-write transaction does not support relationship versioning")
		}
		var err error
		result, err = migrator.MigrateRelationships(ctx, migration)
		return err
	})
	return revision, result, err
}

// RollbackRelationshipMigration runs a relationship migration rollback in a
// single DataLayer transaction.
func RollbackRelationshipMigration(ctx context.Context, dl DataLayer, rollback RelationshipRollback) (datastore.Revision, RelationshipRollbackResult, error) {
	var result RelationshipRollbackResult
	revision, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		migrator, ok := rwt.(RelationshipVersionMigrator)
		if !ok {
			return fmt.Errorf("read-write transaction does not support relationship versioning")
		}
		var err error
		result, err = migrator.RollbackRelationshipMigration(ctx, rollback)
		return err
	})
	return revision, result, err
}

func relationshipReferenceKey(relationship tuple.Relationship) string {
	return relationship.Resource.String() + "@" + relationship.Subject.String()
}
