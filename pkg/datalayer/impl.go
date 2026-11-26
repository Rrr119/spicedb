package datalayer

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

// maxSchemaHashAttempts is the total number of times ReadWriteTx will attempt a transaction
// (including the first) before surfacing ErrSchemaHashPreconditionFailed to the caller.
const maxSchemaHashAttempts = 4

// storedSchemaCache caches stored schemas by hash.
type storedSchemaCache interface {
	GetOrLoad(ctx context.Context, rev datastore.Revision, schemaHash SchemaHash,
		loader func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error)) (*datastore.ReadOnlyStoredSchema, error)
	Set(schemaHash SchemaHash, schema *datastore.ReadOnlyStoredSchema) error
}

// noopSchemaCache is a storedSchemaCache that always delegates to the loader.
type noopSchemaCache struct{}

func (noopSchemaCache) GetOrLoad(ctx context.Context, _ datastore.Revision, _ SchemaHash,
	loader func(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error),
) (*datastore.ReadOnlyStoredSchema, error) {
	return loader(ctx)
}

func (noopSchemaCache) Set(_ SchemaHash, _ *datastore.ReadOnlyStoredSchema) error {
	return nil
}

// DataLayerOption configures a DataLayer.
type DataLayerOption func(*defaultDataLayer)

// WithSchemaMode sets the schema mode for the DataLayer.
func WithSchemaMode(mode SchemaMode) DataLayerOption {
	return func(d *defaultDataLayer) {
		d.schemaMode = mode
	}
}

// WithSchemaCache sets the backing schema cache for the DataLayer.
// When set, ReadStoredSchema calls are cached and WriteStoredSchema updates the cache.
func WithSchemaCache(cache SchemaCache) DataLayerOption {
	return func(d *defaultDataLayer) {
		d.cache = newSchemaHashCache(cache)
	}
}

// NewDataLayer creates a new DataLayer wrapping a datastore.Datastore.
func NewDataLayer(ds datastore.Datastore, opts ...DataLayerOption) DataLayer {
	d := &defaultDataLayer{
		ds:                   ds,
		schemaMode:           SchemaModeReadLegacyWriteLegacy,
		cache:                noopSchemaCache{},
		relationshipVersions: newRelationshipVersionConfig(RelationshipVersionV1),
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// defaultDataLayer wraps a datastore.Datastore and implements DataLayer.
type defaultDataLayer struct {
	ds                   datastore.Datastore
	schemaMode           SchemaMode
	cache                storedSchemaCache
	relationshipVersions relationshipVersionConfig
	lastSchemaHash       atomic.Pointer[string] // most recently observed non-bypass schema hash
}

// observeSchemaHash stores h as the most recently observed schema hash.
// Ignored for empty or bypass-sentinel values.
func (d *defaultDataLayer) observeSchemaHash(h SchemaHash) {
	if h != "" && !h.IsBypassSentinel() {
		s := string(h)
		d.lastSchemaHash.Store(&s)
	}
}

// loadLastSchemaHash returns the most recently observed schema hash, or "" if none.
func (d *defaultDataLayer) loadLastSchemaHash() SchemaHash {
	if p := d.lastSchemaHash.Load(); p != nil {
		return SchemaHash(*p)
	}
	return ""
}

func (d *defaultDataLayer) SnapshotReader(rev datastore.Revision, schemaHash SchemaHash) RevisionedReader {
	if schemaHash == "" {
		_ = spiceerrors.MustBugf("empty string passed as SchemaHash; use a named sentinel")
	}
	d.observeSchemaHash(schemaHash)
	return &revisionedReader{
		reader:               d.ds.SnapshotReader(rev),
		rev:                  rev,
		schemaMode:           d.schemaMode,
		schemaHash:           schemaHash,
		cache:                d.cache,
		relationshipVersions: d.relationshipVersions,
	}
}

func (d *defaultDataLayer) ReadWriteTx(ctx context.Context, fn TxUserFunc, opts ...options.RWTOptionsOption) (datastore.Revision, error) {
	rwtOpts := options.NewRWTOptionsWithOptions(opts...)
	preconditionHash := SchemaHash(rwtOpts.SchemaHashPrecondition)

	// Seed the precondition from the most recently observed hash so ReadSchema
	// inside the transaction can hit the cache without an extra HeadRevision call.
	// Falls back to HeadRevision only when nothing has been observed yet.
	if preconditionHash == "" && d.schemaMode.ReadsFromNew() {
		if h := d.loadLastSchemaHash(); h != "" {
			preconditionHash = h
		} else if result, err := d.ds.HeadRevision(ctx); err == nil && result.SchemaHash != "" {
			preconditionHash = SchemaHash(result.SchemaHash)
			d.observeSchemaHash(preconditionHash)
		}
	}

	var err error
	for range maxSchemaHashAttempts {
		var pendingHash SchemaHash
		var pendingSchema *datastore.ReadOnlyStoredSchema

		// Always pass the current preconditionHash as the last option so that on retries,
		// the refreshed hash overrides whatever the caller originally passed (last option wins).
		dsOpts := make([]options.RWTOptionsOption, len(opts)+1)
		copy(dsOpts, opts)
		dsOpts[len(opts)] = options.WithSchemaHashPrecondition(string(preconditionHash))

		var rev datastore.Revision
		rev, err = d.ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			pendingHash, pendingSchema = "", nil // reset on inner retry
			return fn(ctx, &readWriteTransaction{
				rwt:                  rwt,
				schemaMode:           d.schemaMode,
				cache:                d.cache,
				preconditionHash:     preconditionHash,
				relationshipVersions: d.relationshipVersions,
				onSchemaWritten: func(h SchemaHash, s *datastore.ReadOnlyStoredSchema) {
					pendingHash, pendingSchema = h, s
				},
			})
		}, dsOpts...)

		if err == nil {
			if pendingHash != "" {
				_ = d.cache.Set(pendingHash, pendingSchema)
				d.observeSchemaHash(pendingHash)
			}
			return rev, nil
		}

		// When the schema changed since the caller fetched the hash, transparently
		// refresh the hash and retry so the callback runs against the current schema.
		// fn was never called (assertSchemaHash fires before fn), so retrying is safe.
		if !rwtOpts.DisableRetries &&
			errors.Is(err, datastore.ErrSchemaHashPreconditionFailed) &&
			preconditionHash != "" {
			// Use HeadRevision (not OptimizedRevision) to guarantee we see the
			// hash of the write that just invalidated our precondition, even when
			// the datastore quantizes revisions or caches the optimized revision.
			result, fetchErr := d.ds.HeadRevision(ctx)
			if fetchErr == nil && result.SchemaHash != "" {
				preconditionHash = SchemaHash(result.SchemaHash)
				d.observeSchemaHash(preconditionHash)
				continue
			}
		}

		return datastore.NoRevision, err
	}
	return datastore.NoRevision, err
}

func (d *defaultDataLayer) OptimizedRevision(ctx context.Context) (datastore.Revision, SchemaHash, error) {
	result, err := d.ds.OptimizedRevision(ctx)
	if err != nil {
		return datastore.NoRevision, NoSchemaHashInLegacyMode, err
	}

	if d.schemaMode.ReadsFromNew() && result.SchemaHash != "" {
		hash := SchemaHash(result.SchemaHash)
		d.observeSchemaHash(hash)
		return result.Revision, hash, nil
	}

	return result.Revision, NoSchemaHashInLegacyMode, nil
}

func (d *defaultDataLayer) HeadRevision(ctx context.Context) (datastore.Revision, SchemaHash, error) {
	result, err := d.ds.HeadRevision(ctx)
	if err != nil {
		return datastore.NoRevision, NoSchemaHashInLegacyMode, err
	}

	if d.schemaMode.ReadsFromNew() && result.SchemaHash != "" {
		hash := SchemaHash(result.SchemaHash)
		d.observeSchemaHash(hash)
		return result.Revision, hash, nil
	}

	return result.Revision, NoSchemaHashInLegacyMode, nil
}

func (d *defaultDataLayer) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return d.ds.CheckRevision(ctx, revision)
}

func (d *defaultDataLayer) RevisionFromString(serialized string) (datastore.Revision, error) {
	return d.ds.RevisionFromString(serialized)
}

func (d *defaultDataLayer) Watch(ctx context.Context, afterRevision datastore.Revision, opts datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	version, err := d.relationshipVersions.readVersion(ctx)
	if err != nil {
		changes := make(chan datastore.RevisionChanges)
		errs := make(chan error, 1)
		close(changes)
		errs <- err
		close(errs)
		return changes, errs
	}

	var changes <-chan datastore.RevisionChanges
	var errs <-chan error

	// The datastore's WatchSchema emits changes from the legacy schema storage. When reading
	// schema from the unified stored schema, schema changes must be sourced from it instead.
	if d.schemaMode.ReadsFromNew() && opts.Content&datastore.WatchSchema == datastore.WatchSchema {
		changes, errs = watchViaStoredSchema(ctx, d.ds, afterRevision, opts)
	} else {
		changes, errs = d.ds.Watch(ctx, afterRevision, opts)
	}

	return relationshipChangesForVersion(ctx, changes, errs, version, opts.Content)
}

func (d *defaultDataLayer) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	return d.ds.ReadyState(ctx)
}

func (d *defaultDataLayer) Features(ctx context.Context) (*datastore.Features, error) {
	return d.ds.Features(ctx)
}

func (d *defaultDataLayer) OfflineFeatures() (*datastore.Features, error) {
	return d.ds.OfflineFeatures()
}

func (d *defaultDataLayer) Statistics(ctx context.Context) (datastore.Stats, error) {
	return d.ds.Statistics(ctx)
}

func (d *defaultDataLayer) UniqueID(ctx context.Context) (string, error) {
	return d.ds.UniqueID(ctx)
}

func (d *defaultDataLayer) MetricsID() (string, error) {
	return d.ds.MetricsID()
}

func (d *defaultDataLayer) Close() error {
	return d.ds.Close()
}

// revisionedReader wraps a datastore.Reader and implements RevisionedReader.
type revisionedReader struct {
	reader               datastore.Reader
	rev                  datastore.Revision
	schemaMode           SchemaMode
	schemaHash           SchemaHash
	cache                storedSchemaCache
	relationshipVersions relationshipVersionConfig
}

// The reads below assert, in CI builds, that the calling goroutine is not iterating over a
// relationships query: SQL datastores hold a pooled connection for the duration of an iteration,
// so a read issued during one needs a second connection and can deadlock the pool (#3433).

func (r *revisionedReader) ReadSchema(ctx context.Context) (SchemaReader, error) {
	assertNoHeldIterator("ReadSchema")
	if r.schemaMode.ReadsFromNew() {
		return newStoredSchemaReaderAdapter(ctx, r.reader, r.schemaHash, r.rev, r.cache)
	}
	return &legacySchemaReaderAdapter{legacyReader: r.reader}, nil
}

func (r *revisionedReader) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	version, err := r.relationshipVersions.readVersion(ctx)
	if err != nil {
		return nil, err
	}
	return r.QueryRelationshipsAtVersion(ctx, version, filter, opts...)
}

func (r *revisionedReader) QueryRelationshipsAtVersion(
	ctx context.Context,
	version RelationshipVersion,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	version = normalizeRelationshipVersion(version)
	if !version.IsValid() {
		return nil, fmt.Errorf("invalid relationship version %q", version)
	}

	it, err := r.reader.QueryRelationships(
		ctx,
		version.translateRelationshipsFilter(filter),
		version.translateQueryOptions(opts...)...,
	)
	if err != nil {
		return nil, err
	}
	return trackHeldIterator("QueryRelationships", relationshipIteratorForVersion(it, version)), nil
}

func (r *revisionedReader) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, opts ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	version, err := r.relationshipVersions.readVersion(ctx)
	if err != nil {
		return nil, err
	}
	return r.ReverseQueryRelationshipsAtVersion(ctx, version, subjectsFilter, opts...)
}

func (r *revisionedReader) ReverseQueryRelationshipsAtVersion(
	ctx context.Context,
	version RelationshipVersion,
	subjectsFilter datastore.SubjectsFilter,
	opts ...options.ReverseQueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	version = normalizeRelationshipVersion(version)
	if !version.IsValid() {
		return nil, fmt.Errorf("invalid relationship version %q", version)
	}

	it, err := r.reader.ReverseQueryRelationships(
		ctx,
		version.translateSubjectsFilter(subjectsFilter),
		version.translateReverseQueryOptions(opts...)...,
	)
	if err != nil {
		return nil, err
	}
	return trackHeldIterator("ReverseQueryRelationships", relationshipIteratorForVersion(it, version)), nil
}

func (r *revisionedReader) CountRelationships(ctx context.Context, name string) (int, error) {
	assertNoHeldIterator("CountRelationships")
	return r.reader.CountRelationships(ctx, name)
}

func (r *revisionedReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	assertNoHeldIterator("LookupCounters")
	return r.reader.LookupCounters(ctx)
}

// readWriteTransaction wraps a datastore.ReadWriteTransaction and implements ReadWriteTransaction.
type readWriteTransaction struct {
	rwt                  datastore.ReadWriteTransaction
	schemaMode           SchemaMode
	cache                storedSchemaCache
	relationshipVersions relationshipVersionConfig

	// onSchemaWritten is called by WriteSchema after a successful datastore write.
	// ReadWriteTx sets this to capture the written schema for post-commit cache update.
	onSchemaWritten func(SchemaHash, *datastore.ReadOnlyStoredSchema)

	// preconditionHash, when non-empty, is the hash the datastore asserted at tx open time
	// (via WithSchemaHashPrecondition). ReadSchema uses it directly for cache lookup.
	preconditionHash SchemaHash
}

func (t *readWriteTransaction) ReadSchema(ctx context.Context) (SchemaReader, error) {
	if t.schemaMode.ReadsFromNew() {
		hash := t.preconditionHash
		if hash == "" {
			hash = NoSchemaHashInTransaction
		}
		return newStoredSchemaReaderAdapter(ctx, t.rwt, hash, datastore.NoRevision, t.cache)
	}
	return &legacySchemaReaderAdapter{legacyReader: t.rwt}, nil
}

func (t *readWriteTransaction) QueryRelationships(ctx context.Context, filter datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	version, err := t.relationshipVersions.readVersion(ctx)
	if err != nil {
		return nil, err
	}
	return t.QueryRelationshipsAtVersion(ctx, version, filter, opts...)
}

func (t *readWriteTransaction) QueryRelationshipsAtVersion(
	ctx context.Context,
	version RelationshipVersion,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	version = normalizeRelationshipVersion(version)
	if !version.IsValid() {
		return nil, fmt.Errorf("invalid relationship version %q", version)
	}
	it, err := t.rwt.QueryRelationships(
		ctx,
		version.translateRelationshipsFilter(filter),
		version.translateQueryOptions(opts...)...,
	)
	if err != nil {
		return nil, err
	}
	return relationshipIteratorForVersion(it, version), nil
}

func (t *readWriteTransaction) ReverseQueryRelationships(ctx context.Context, subjectsFilter datastore.SubjectsFilter, opts ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	version, err := t.relationshipVersions.readVersion(ctx)
	if err != nil {
		return nil, err
	}
	return t.ReverseQueryRelationshipsAtVersion(ctx, version, subjectsFilter, opts...)
}

func (t *readWriteTransaction) ReverseQueryRelationshipsAtVersion(
	ctx context.Context,
	version RelationshipVersion,
	subjectsFilter datastore.SubjectsFilter,
	opts ...options.ReverseQueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	version = normalizeRelationshipVersion(version)
	if !version.IsValid() {
		return nil, fmt.Errorf("invalid relationship version %q", version)
	}
	it, err := t.rwt.ReverseQueryRelationships(
		ctx,
		version.translateSubjectsFilter(subjectsFilter),
		version.translateReverseQueryOptions(opts...)...,
	)
	if err != nil {
		return nil, err
	}
	return relationshipIteratorForVersion(it, version), nil
}

func (t *readWriteTransaction) CountRelationships(ctx context.Context, name string) (int, error) {
	return t.rwt.CountRelationships(ctx, name)
}

func (t *readWriteTransaction) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	return t.rwt.LookupCounters(ctx)
}

func (t *readWriteTransaction) WriteRelationships(ctx context.Context, mutations []tuple.RelationshipUpdate) error {
	versions, err := t.relationshipVersions.writeVersions(ctx)
	if err != nil {
		return err
	}

	if len(versions) == 1 && versions[0] == RelationshipVersionV1 {
		return t.rwt.WriteRelationships(ctx, mutations)
	}

	for _, version := range versions {
		translated := make([]tuple.RelationshipUpdate, 0, len(mutations))
		for _, mutation := range mutations {
			mutation.Relationship = version.toStorageRelationship(mutation.Relationship)
			translated = append(translated, mutation)
		}
		if err := t.rwt.WriteRelationships(ctx, translated); err != nil {
			return err
		}
	}
	return nil
}

func (t *readWriteTransaction) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter, opts ...options.DeleteOptionsOption) (datastore.DeleteRelationshipsResult, error) {
	versions, err := t.relationshipVersions.writeVersions(ctx)
	if err != nil {
		return datastore.DeleteRelationshipsResult{}, err
	}
	return t.deleteRelationshipsAtVersion(ctx, versions, filter, opts...)
}

func (t *readWriteTransaction) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	versions, err := t.relationshipVersions.writeVersions(ctx)
	if err != nil {
		return 0, err
	}

	if len(versions) == 1 && versions[0] == RelationshipVersionV1 {
		return t.rwt.BulkLoad(ctx, iter)
	}

	relationships := make([]tuple.Relationship, 0)
	for {
		relationship, err := iter.Next(ctx)
		if err != nil {
			return 0, err
		}
		if relationship == nil {
			break
		}
		relationships = append(relationships, *relationship)
	}

	var total uint64
	for _, version := range versions {
		source := &sliceBulkRelationshipSource{relationships: relationships, version: version}
		written, err := t.rwt.BulkLoad(ctx, source)
		if err != nil {
			return total, err
		}
		if written > total {
			total = written
		}
	}
	return total, nil
}

type sliceBulkRelationshipSource struct {
	relationships []tuple.Relationship
	index         int
	version       RelationshipVersion
}

func (s *sliceBulkRelationshipSource) Next(context.Context) (*tuple.Relationship, error) {
	if s.index >= len(s.relationships) {
		return nil, nil
	}
	relationship := s.version.toStorageRelationship(s.relationships[s.index])
	s.index++
	return &relationship, nil
}

func (t *readWriteTransaction) deleteRelationshipsAtVersion(
	ctx context.Context,
	versions []RelationshipVersion,
	filter *v1.RelationshipFilter,
	opts ...options.DeleteOptionsOption,
) (datastore.DeleteRelationshipsResult, error) {
	if filter == nil {
		filter = &v1.RelationshipFilter{}
	}

	deleteOptions := options.NewDeleteOptionsWithOptionsAndDefaults(opts...)

	if len(versions) == 1 && versions[0] == RelationshipVersionV1 {
		return t.rwt.DeleteRelationships(ctx, filter, opts...)
	}

	var result datastore.DeleteRelationshipsResult
	for _, version := range versions {
		versionResult, err := t.deleteRelationshipsForVersion(ctx, version, filter, deleteOptions)
		if err != nil {
			return datastore.DeleteRelationshipsResult{}, err
		}
		if versionResult.NumDeleted > result.NumDeleted {
			result.NumDeleted = versionResult.NumDeleted
		}
		result.LimitReached = result.LimitReached || versionResult.LimitReached
		if result.Cursor == nil {
			result.Cursor = version.logicalCursor(versionResult.Cursor)
		}
	}
	return result, nil
}

func (t *readWriteTransaction) deleteRelationshipsForVersion(
	ctx context.Context,
	version RelationshipVersion,
	filter *v1.RelationshipFilter,
	deleteOptions *options.DeleteOptions,
) (datastore.DeleteRelationshipsResult, error) {
	if version == RelationshipVersionV1 && filter.ResourceType != "" {
		translatedOptions := *deleteOptions
		if translatedOptions.DeleteAfter != nil {
			translatedOptions.DeleteAfter = version.translateCursor(translatedOptions.DeleteAfter)
		}
		result, err := t.rwt.DeleteRelationships(ctx, filter, translatedOptions.ToOption())
		if err != nil {
			return datastore.DeleteRelationshipsResult{}, err
		}
		result.Cursor = version.logicalCursor(result.Cursor)
		return result, nil
	}

	if deleteOptions.IsCursoredDelete() {
		return datastore.DeleteRelationshipsResult{}, datastore.NewCursoredDeleteNotSupportedErr(
			"relationship-version-" + version.String(),
		)
	}

	dsFilter, err := datastore.RelationshipsFilterFromPublicFilter(filter)
	if err != nil {
		return datastore.DeleteRelationshipsResult{}, err
	}

	iter, err := t.QueryRelationshipsAtVersion(ctx, version, dsFilter)
	if err != nil {
		return datastore.DeleteRelationshipsResult{}, err
	}

	var limit uint64
	if deleteOptions.DeleteLimit != nil {
		limit = *deleteOptions.DeleteLimit
	}

	mutations := make([]tuple.RelationshipUpdate, 0)
	var last tuple.Relationship
	limitReached := false
	for relationship, iterErr := range iter {
		if iterErr != nil {
			return datastore.DeleteRelationshipsResult{}, iterErr
		}
		if limit > 0 && uint64(len(mutations)) == limit {
			limitReached = true
			break
		}
		mutations = append(mutations, tuple.Delete(version.toStorageRelationship(relationship)))
		last = relationship
	}

	if len(mutations) > 0 {
		if err := t.rwt.WriteRelationships(ctx, mutations); err != nil {
			return datastore.DeleteRelationshipsResult{}, err
		}
	}

	result := datastore.DeleteRelationshipsResult{
		NumDeleted:   uint64(len(mutations)),
		LimitReached: limitReached,
	}
	if len(mutations) > 0 {
		result.Cursor = options.ToCursor(last)
	}
	return result, nil
}

func (t *readWriteTransaction) MigrateRelationships(
	ctx context.Context,
	migration RelationshipMigration,
) (RelationshipMigrationResult, error) {
	source := normalizeRelationshipVersion(migration.Source)
	target := normalizeRelationshipVersion(migration.Target)
	if !source.IsValid() {
		return RelationshipMigrationResult{}, fmt.Errorf("invalid source relationship version %q", migration.Source)
	}
	if !target.IsValid() {
		return RelationshipMigrationResult{}, fmt.Errorf("invalid target relationship version %q", migration.Target)
	}
	if source == target {
		return RelationshipMigrationResult{}, fmt.Errorf("source and target relationship versions must differ")
	}

	result := RelationshipMigrationResult{}
	if migration.Schema != nil {
		if _, err := t.WriteSchema(
			ctx,
			migration.Schema.Definitions,
			migration.Schema.SchemaText,
			migration.Schema.CaveatTypeSet,
		); err != nil {
			return RelationshipMigrationResult{}, err
		}
		result.SchemaWritten = true
	}

	iter, err := t.QueryRelationshipsAtVersion(ctx, source, migration.Filter)
	if err != nil {
		return RelationshipMigrationResult{}, err
	}

	relationships := make([]tuple.Relationship, 0)
	sourceReferences := make(map[string]struct{})
	for relationship, iterErr := range iter {
		if iterErr != nil {
			return RelationshipMigrationResult{}, iterErr
		}
		relationships = append(relationships, relationship)
		sourceReferences[relationshipReferenceKey(relationship)] = struct{}{}
	}

	updates := make([]tuple.RelationshipUpdate, 0, len(relationships))
	for _, relationship := range relationships {
		updates = append(updates, tuple.Touch(target.toStorageRelationship(relationship)))
	}

	targetIter, err := t.QueryRelationshipsAtVersion(ctx, target, migration.Filter)
	if err != nil {
		return RelationshipMigrationResult{}, err
	}
	staleDeletions := make([]tuple.RelationshipUpdate, 0)
	for relationship, iterErr := range targetIter {
		if iterErr != nil {
			return RelationshipMigrationResult{}, iterErr
		}
		if _, ok := sourceReferences[relationshipReferenceKey(relationship)]; ok {
			continue
		}
		staleDeletions = append(staleDeletions, tuple.Delete(target.toStorageRelationship(relationship)))
	}

	if len(staleDeletions) > 0 {
		if err := t.rwt.WriteRelationships(ctx, staleDeletions); err != nil {
			return RelationshipMigrationResult{}, err
		}
	}
	if len(updates) > 0 {
		if err := t.rwt.WriteRelationships(ctx, updates); err != nil {
			return RelationshipMigrationResult{}, err
		}
	}

	result.Copied = uint64(len(updates))
	result.RemovedStale = uint64(len(staleDeletions))
	return result, nil
}

func (t *readWriteTransaction) RollbackRelationshipMigration(
	ctx context.Context,
	rollback RelationshipRollback,
) (RelationshipRollbackResult, error) {
	target := normalizeRelationshipVersion(rollback.Target)
	if !target.IsValid() {
		return RelationshipRollbackResult{}, fmt.Errorf("invalid target relationship version %q", rollback.Target)
	}

	source := normalizeRelationshipVersion(rollback.Source)
	if rollback.Source != "" && !source.IsValid() {
		return RelationshipRollbackResult{}, fmt.Errorf("invalid source relationship version %q", rollback.Source)
	}
	if rollback.Source != "" && source == target {
		return RelationshipRollbackResult{}, fmt.Errorf("source and target relationship versions must differ")
	}

	result := RelationshipRollbackResult{}
	if rollback.Schema != nil {
		if _, err := t.WriteSchema(
			ctx,
			rollback.Schema.Definitions,
			rollback.Schema.SchemaText,
			rollback.Schema.CaveatTypeSet,
		); err != nil {
			return RelationshipRollbackResult{}, err
		}
		result.SchemaWritten = true
	}

	targetIter, err := t.QueryRelationshipsAtVersion(ctx, target, rollback.Filter)
	if err != nil {
		return RelationshipRollbackResult{}, err
	}

	targetRelationships := make([]tuple.Relationship, 0)
	for relationship, iterErr := range targetIter {
		if iterErr != nil {
			return RelationshipRollbackResult{}, iterErr
		}
		targetRelationships = append(targetRelationships, relationship)
	}

	var sourceReferences map[string]struct{}
	if rollback.Source != "" {
		sourceIter, err := t.QueryRelationshipsAtVersion(ctx, source, rollback.Filter)
		if err != nil {
			return RelationshipRollbackResult{}, err
		}
		sourceReferences = make(map[string]struct{})
		for relationship, iterErr := range sourceIter {
			if iterErr != nil {
				return RelationshipRollbackResult{}, iterErr
			}
			sourceReferences[relationshipReferenceKey(relationship)] = struct{}{}
		}
	}

	deletions := make([]tuple.RelationshipUpdate, 0, len(targetRelationships))
	for _, relationship := range targetRelationships {
		if sourceReferences != nil {
			if _, ok := sourceReferences[relationshipReferenceKey(relationship)]; !ok {
				continue
			}
		}
		deletions = append(deletions, tuple.Delete(target.toStorageRelationship(relationship)))
	}

	if len(deletions) > 0 {
		if err := t.rwt.WriteRelationships(ctx, deletions); err != nil {
			return RelationshipRollbackResult{}, err
		}
	}
	result.Deleted = uint64(len(deletions))
	return result, nil
}

func (t *readWriteTransaction) WriteSchema(ctx context.Context, definitions []datastore.SchemaDefinition, schemaString string, caveatTypeSet *caveattypes.TypeSet) (SchemaHash, error) {
	// Write to legacy storage if mode requires it
	if t.schemaMode.WritesToLegacy() {
		if err := writeSchemaViaLegacy(ctx, t.rwt, t.rwt, definitions); err != nil {
			return "", err
		}
	}

	// Write to unified storage if mode requires it
	if t.schemaMode.WritesToNew() {
		schemaHash, schema, err := WriteSchemaViaStoredSchema(ctx, t.rwt, definitions, schemaString)
		if err != nil {
			return "", err
		}
		if t.onSchemaWritten != nil {
			t.onSchemaWritten(schemaHash, schema)
		}
		return schemaHash, nil
	}

	// Legacy-only storage produces no unified schema hash.
	return NoSchemaHashInLegacyMode, nil
}

func (t *readWriteTransaction) LegacySchemaWriter() LegacySchemaWriter {
	return &legacySchemaWriterPassthrough{rwt: t.rwt}
}

type legacySchemaWriterPassthrough struct {
	rwt datastore.ReadWriteTransaction
}

func (w *legacySchemaWriterPassthrough) LegacyWriteCaveats(ctx context.Context, caveats []*core.CaveatDefinition) error {
	return w.rwt.LegacyWriteCaveats(ctx, caveats)
}

func (w *legacySchemaWriterPassthrough) LegacyWriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	return w.rwt.LegacyWriteNamespaces(ctx, newConfigs...)
}

func (w *legacySchemaWriterPassthrough) LegacyDeleteCaveats(ctx context.Context, names []string) error {
	return w.rwt.LegacyDeleteCaveats(ctx, names)
}

func (w *legacySchemaWriterPassthrough) LegacyDeleteNamespaces(ctx context.Context, nsNames []string, delOption datastore.DeleteNamespacesRelationshipsOption) error {
	return w.rwt.LegacyDeleteNamespaces(ctx, nsNames, delOption)
}

func (t *readWriteTransaction) RegisterCounter(ctx context.Context, name string, filter *core.RelationshipFilter) error {
	return t.rwt.RegisterCounter(ctx, name, filter)
}

func (t *readWriteTransaction) UnregisterCounter(ctx context.Context, name string) error {
	return t.rwt.UnregisterCounter(ctx, name)
}

func (t *readWriteTransaction) StoreCounterValue(ctx context.Context, name string, value int, computedAtRevision datastore.Revision) error {
	return t.rwt.StoreCounterValue(ctx, name, value, computedAtRevision)
}

// datastoreUnwrapper is implemented by DataLayer wrappers to expose the underlying datastore.
type datastoreUnwrapper interface {
	unwrapDatastore() datastore.Datastore
}

func (d *defaultDataLayer) unwrapDatastore() datastore.Datastore { return d.ds }

// UnwrapDatastore extracts the underlying datastore.Datastore from a DataLayer.
// This is for internal use by code that needs raw datastore access (e.g., schema operations).
func UnwrapDatastore(dl DataLayer) datastore.Datastore {
	if u, ok := dl.(datastoreUnwrapper); ok {
		return u.unwrapDatastore()
	}
	return nil
}

// NewReadOnlyDataLayer creates a DataLayer from a ReadOnlyDatastore.
// ReadWriteTx will return a readonly error.
func NewReadOnlyDataLayer(ds datastore.ReadOnlyDatastore) DataLayer {
	return &readOnlyDatastoreAdapter{ds: ds}
}

type readOnlyDatastoreAdapter struct {
	ds datastore.ReadOnlyDatastore
}

func (r *readOnlyDatastoreAdapter) SnapshotReader(rev datastore.Revision, schemaHash SchemaHash) RevisionedReader {
	if schemaHash == "" {
		_ = spiceerrors.MustBugf("empty string passed as SchemaHash; use a named sentinel")
	}
	return &revisionedReader{
		reader:     r.ds.SnapshotReader(rev),
		rev:        rev,
		schemaMode: SchemaModeReadLegacyWriteLegacy,
		schemaHash: schemaHash,
	}
}

func (r *readOnlyDatastoreAdapter) ReadWriteTx(_ context.Context, _ TxUserFunc, _ ...options.RWTOptionsOption) (datastore.Revision, error) {
	return datastore.NoRevision, datastore.NewReadonlyErr()
}

func (r *readOnlyDatastoreAdapter) OptimizedRevision(ctx context.Context) (datastore.Revision, SchemaHash, error) {
	result, err := r.ds.OptimizedRevision(ctx)
	if err != nil {
		return datastore.NoRevision, NoSchemaHashInLegacyMode, err
	}
	return result.Revision, NoSchemaHashInLegacyMode, nil
}

func (r *readOnlyDatastoreAdapter) HeadRevision(ctx context.Context) (datastore.Revision, SchemaHash, error) {
	result, err := r.ds.HeadRevision(ctx)
	if err != nil {
		return datastore.NoRevision, NoSchemaHashInLegacyMode, err
	}
	return result.Revision, NoSchemaHashInLegacyMode, nil
}

func (r *readOnlyDatastoreAdapter) CheckRevision(ctx context.Context, revision datastore.Revision) error {
	return r.ds.CheckRevision(ctx, revision)
}

func (r *readOnlyDatastoreAdapter) RevisionFromString(serialized string) (datastore.Revision, error) {
	return r.ds.RevisionFromString(serialized)
}

func (r *readOnlyDatastoreAdapter) Watch(ctx context.Context, afterRevision datastore.Revision, opts datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	version, ok := RelationshipVersionFromContext(ctx)
	if !ok {
		version = RelationshipVersionV1
	}
	if !version.IsValid() {
		changes := make(chan datastore.RevisionChanges)
		errs := make(chan error, 1)
		close(changes)
		errs <- fmt.Errorf("invalid relationship version %q", version)
		close(errs)
		return changes, errs
	}
	changes, errs := r.ds.Watch(ctx, afterRevision, opts)
	return relationshipChangesForVersion(ctx, changes, errs, version, opts.Content)
}

func (r *readOnlyDatastoreAdapter) ReadyState(ctx context.Context) (datastore.ReadyState, error) {
	return r.ds.ReadyState(ctx)
}

func (r *readOnlyDatastoreAdapter) Features(ctx context.Context) (*datastore.Features, error) {
	return r.ds.Features(ctx)
}

func (r *readOnlyDatastoreAdapter) OfflineFeatures() (*datastore.Features, error) {
	return r.ds.OfflineFeatures()
}

func (r *readOnlyDatastoreAdapter) Statistics(ctx context.Context) (datastore.Stats, error) {
	return r.ds.Statistics(ctx)
}

func (r *readOnlyDatastoreAdapter) UniqueID(ctx context.Context) (string, error) {
	return r.ds.UniqueID(ctx)
}

func (r *readOnlyDatastoreAdapter) MetricsID() (string, error) {
	return r.ds.MetricsID()
}

func (r *readOnlyDatastoreAdapter) Close() error {
	return r.ds.Close()
}
