package datalayer

import (
	"context"
	"errors"
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

func newRelationshipVersionTestDataLayer(
	t *testing.T,
	readVersion RelationshipVersion,
	dualWrite ...RelationshipVersion,
) (DataLayer, datastore.Datastore) {
	t.Helper()
	ds := newTestDatastore(t)
	dl := NewDataLayer(
		ds,
		WithSchemaMode(SchemaModeReadLegacyWriteBoth),
		WithRelationshipVersioning(readVersion, dualWrite...),
	)
	return dl, ds
}

func writeTestRelationships(
	t *testing.T,
	dl DataLayer,
	ctx context.Context,
	relationships ...tuple.Relationship,
) {
	t.Helper()
	_, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		updates := make([]tuple.RelationshipUpdate, 0, len(relationships))
		for _, relationship := range relationships {
			updates = append(updates, tuple.Touch(relationship))
		}
		return rwt.WriteRelationships(ctx, updates)
	})
	require.NoError(t, err)
}

func readRelationshipStrings(
	t *testing.T,
	reader RevisionedReader,
	version RelationshipVersion,
) []string {
	t.Helper()
	iter, err := QueryRelationshipsAtVersion(
		t.Context(),
		reader,
		version,
		datastore.RelationshipsFilter{OptionalResourceType: "document"},
	)
	require.NoError(t, err)

	relationships, err := datastore.IteratorToSlice(iter)
	require.NoError(t, err)

	result := make([]string, 0, len(relationships))
	for _, relationship := range relationships {
		serialized, err := tuple.String(relationship)
		require.NoError(t, err)
		result = append(result, serialized)
	}
	return result
}

func TestRelationshipVersionDualWriteAndRead(t *testing.T) {
	dl, _ := newRelationshipVersionTestDataLayer(
		t,
		RelationshipVersionV1,
		RelationshipVersionV2,
		RelationshipVersionV3,
	)

	relationship := tuple.MustParse("document:doc1#viewer@user:tom")
	writeTestRelationships(t, dl, t.Context(), relationship)

	revision, _, err := dl.HeadRevision(t.Context())
	require.NoError(t, err)
	reader := dl.SnapshotReader(revision, NoSchemaHashForTesting)

	expected := []string{"document:doc1#viewer@user:tom"}
	for _, version := range SupportedRelationshipVersions() {
		require.Equal(t, expected, readRelationshipStrings(t, reader, version), "version %s", version)

		reverseIter, err := ReverseQueryRelationshipsAtVersion(
			t.Context(),
			reader,
			version,
			datastore.SubjectsFilter{SubjectType: "user"},
		)
		require.NoError(t, err)
		reverseRelationships, err := datastore.IteratorToSlice(reverseIter)
		require.NoError(t, err)
		require.Len(t, reverseRelationships, 1)
	}

	iter, err := reader.QueryRelationships(
		t.Context(),
		datastore.RelationshipsFilter{},
	)
	require.NoError(t, err)
	allDefaultVersionRelationships, err := datastore.IteratorToSlice(iter)
	require.NoError(t, err)
	require.Len(t, allDefaultVersionRelationships, 1)
}

func TestRelationshipVersionPhysicalNamesValidate(t *testing.T) {
	relationship := tuple.MustParse("document:doc1#viewer@user:tom")
	for _, version := range []RelationshipVersion{RelationshipVersionV2, RelationshipVersionV3} {
		physical := version.toStorageRelationship(relationship)
		require.NoError(t, protovalidate.Validate(physical.ToCoreTuple()), "version %s", version)
	}
}

func TestRelationshipVersionBulkLoadWritesAllVersions(t *testing.T) {
	dl, _ := newRelationshipVersionTestDataLayer(
		t,
		RelationshipVersionV1,
		RelationshipVersionV2,
		RelationshipVersionV3,
	)

	relationships := []tuple.Relationship{
		tuple.MustParse("document:doc1#viewer@user:tom"),
		tuple.MustParse("document:doc2#viewer@user:sarah"),
	}
	_, err := dl.ReadWriteTx(t.Context(), func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.BulkLoad(ctx, &stubBulkSourceDL{rels: relationships})
		return err
	})
	require.NoError(t, err)

	revision, _, err := dl.HeadRevision(t.Context())
	require.NoError(t, err)
	reader := dl.SnapshotReader(revision, NoSchemaHashForTesting)
	for _, version := range SupportedRelationshipVersions() {
		require.Len(t, readRelationshipStrings(t, reader, version), 2, "version %s", version)
	}
}

func TestRelationshipVersionMetadataInterceptor(t *testing.T) {
	interceptor := UnaryServerInterceptor(nil)

	ctx := metadata.NewIncomingContext(
		t.Context(),
		metadata.Pairs(RelationshipVersionMetadataKey, string(RelationshipVersionV2)),
	)
	var selected RelationshipVersion
	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
		selected, _ = RelationshipVersionFromContext(ctx)
		return nil, nil
	})
	require.NoError(t, err)
	require.Equal(t, RelationshipVersionV2, selected)

	invalidCtx := metadata.NewIncomingContext(
		t.Context(),
		metadata.Pairs(RelationshipVersionMetadataKey, "v99"),
	)
	_, err = interceptor(invalidCtx, nil, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) {
		return nil, nil
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestRelationshipVersionDeleteRemovesAllWriteVersions(t *testing.T) {
	dl, _ := newRelationshipVersionTestDataLayer(
		t,
		RelationshipVersionV1,
		RelationshipVersionV2,
		RelationshipVersionV3,
	)

	relationship := tuple.MustParse("document:doc1#viewer@user:tom")
	writeTestRelationships(t, dl, t.Context(), relationship)

	_, err := dl.ReadWriteTx(t.Context(), func(ctx context.Context, rwt ReadWriteTransaction) error {
		_, err := rwt.DeleteRelationships(ctx, &v1.RelationshipFilter{
			ResourceType:       "document",
			OptionalResourceId: "doc1",
		})
		return err
	})
	require.NoError(t, err)

	revision, _, err := dl.HeadRevision(t.Context())
	require.NoError(t, err)
	reader := dl.SnapshotReader(revision, NoSchemaHashForTesting)
	for _, version := range SupportedRelationshipVersions() {
		require.Empty(t, readRelationshipStrings(t, reader, version), "version %s", version)
	}
}

func TestRelationshipMigrationIsAtomicAndRollbackSafe(t *testing.T) {
	dl, _ := newRelationshipVersionTestDataLayer(t, RelationshipVersionV1)

	relationships := []tuple.Relationship{
		tuple.MustParse("document:doc1#viewer@user:tom"),
		tuple.MustParse("document:doc2#viewer@user:sarah"),
	}
	writeTestRelationships(t, dl, t.Context(), relationships...)
	writeTestRelationships(
		t,
		dl,
		ContextWithRelationshipVersion(t.Context(), RelationshipVersionV2),
		tuple.MustParse("document:stale#viewer@user:old"),
	)

	interrupted := errors.New("simulated migration interruption")
	_, err := dl.ReadWriteTx(t.Context(), func(ctx context.Context, rwt ReadWriteTransaction) error {
		migrator := rwt.(RelationshipVersionMigrator)
		_, err := migrator.MigrateRelationships(ctx, RelationshipMigration{
			Source: RelationshipVersionV1,
			Target: RelationshipVersionV2,
		})
		require.NoError(t, err)
		return interrupted
	})
	require.ErrorIs(t, err, interrupted)

	revision, _, err := dl.HeadRevision(t.Context())
	require.NoError(t, err)
	reader := dl.SnapshotReader(revision, NoSchemaHashForTesting)
	require.Len(t, readRelationshipStrings(t, reader, RelationshipVersionV1), 2)
	require.Len(t, readRelationshipStrings(t, reader, RelationshipVersionV2), 1)

	_, result, err := MigrateRelationships(t.Context(), dl, RelationshipMigration{
		Source: RelationshipVersionV1,
		Target: RelationshipVersionV2,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), result.Copied)
	require.Equal(t, uint64(1), result.RemovedStale)

	revision, _, err = dl.HeadRevision(t.Context())
	require.NoError(t, err)
	reader = dl.SnapshotReader(revision, NoSchemaHashForTesting)
	require.Len(t, readRelationshipStrings(t, reader, RelationshipVersionV1), 2)
	require.Len(t, readRelationshipStrings(t, reader, RelationshipVersionV2), 2)

	targetOnly := tuple.MustParse("document:doc3#viewer@user:target-only")
	writeTestRelationships(
		t,
		dl,
		ContextWithRelationshipVersion(t.Context(), RelationshipVersionV2),
		targetOnly,
	)

	_, rollbackResult, err := RollbackRelationshipMigration(t.Context(), dl, RelationshipRollback{
		Source: RelationshipVersionV1,
		Target: RelationshipVersionV2,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), rollbackResult.Deleted)

	revision, _, err = dl.HeadRevision(t.Context())
	require.NoError(t, err)
	reader = dl.SnapshotReader(revision, NoSchemaHashForTesting)
	require.Len(t, readRelationshipStrings(t, reader, RelationshipVersionV1), 2)
	require.Equal(t, []string{"document:doc3#viewer@user:target-only"}, readRelationshipStrings(t, reader, RelationshipVersionV2))
}

func TestSchemaAndRelationshipMigrationCommitTogether(t *testing.T) {
	dl, _ := newRelationshipVersionTestDataLayer(t, RelationshipVersionV1)

	oldDefinitions, oldSchemaText := testSchemaDefinitions(t)
	_, err := dl.ReadWriteTx(t.Context(), func(ctx context.Context, rwt ReadWriteTransaction) error {
		if _, err := rwt.WriteSchema(ctx, oldDefinitions, oldSchemaText, nil); err != nil {
			return err
		}
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Touch(tuple.MustParse("document:doc1#viewer@user:tom")),
		})
	})
	require.NoError(t, err)

	const newSchemaText = `definition user {}
definition document {
	relation viewer: user
}
definition folder {}`
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: newSchemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	newDefinitions := make([]datastore.SchemaDefinition, 0, len(compiled.ObjectDefinitions)+len(compiled.CaveatDefinitions))
	for _, caveat := range compiled.CaveatDefinitions {
		newDefinitions = append(newDefinitions, caveat)
	}
	for _, object := range compiled.ObjectDefinitions {
		newDefinitions = append(newDefinitions, object)
	}

	interrupted := errors.New("simulated schema migration interruption")
	_, err = dl.ReadWriteTx(t.Context(), func(ctx context.Context, rwt ReadWriteTransaction) error {
		migrator := rwt.(RelationshipVersionMigrator)
		_, err := migrator.MigrateRelationships(ctx, RelationshipMigration{
			Source: RelationshipVersionV1,
			Target: RelationshipVersionV2,
			Schema: &RelationshipSchemaMigration{
				Definitions: newDefinitions,
				SchemaText:  newSchemaText,
			},
		})
		require.NoError(t, err)
		return interrupted
	})
	require.ErrorIs(t, err, interrupted)

	headRevision, _, err := dl.HeadRevision(t.Context())
	require.NoError(t, err)
	schemaReader, err := dl.SnapshotReader(headRevision, NoSchemaHashForTesting).ReadSchema(t.Context())
	require.NoError(t, err)
	persistedSchema, err := schemaReader.SchemaText(t.Context())
	require.NoError(t, err)
	require.NotContains(t, persistedSchema, "definition folder")

	_, result, err := MigrateRelationships(t.Context(), dl, RelationshipMigration{
		Source: RelationshipVersionV1,
		Target: RelationshipVersionV2,
		Schema: &RelationshipSchemaMigration{
			Definitions: newDefinitions,
			SchemaText:  newSchemaText,
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), result.Copied)
	require.True(t, result.SchemaWritten)

	headRevision, _, err = dl.HeadRevision(t.Context())
	require.NoError(t, err)
	schemaReader, err = dl.SnapshotReader(headRevision, NoSchemaHashForTesting).ReadSchema(t.Context())
	require.NoError(t, err)
	persistedSchema, err = schemaReader.SchemaText(t.Context())
	require.NoError(t, err)
	require.Contains(t, persistedSchema, "definition folder")
}
