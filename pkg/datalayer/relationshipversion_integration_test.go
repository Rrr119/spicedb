package datalayer_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/graph/computed"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

func compileRelationshipVersionSchema(t *testing.T, schemaText string) ([]datastore.SchemaDefinition, string) {
	t.Helper()
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	definitions := make([]datastore.SchemaDefinition, 0, len(compiled.ObjectDefinitions)+len(compiled.CaveatDefinitions))
	for _, caveat := range compiled.CaveatDefinitions {
		definitions = append(definitions, caveat)
	}
	for _, object := range compiled.ObjectDefinitions {
		definitions = append(definitions, object)
	}
	return definitions, schemaText
}

func TestThreeSchemaVersionsProduceConsistentPermissions(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	dl := datalayer.NewDataLayer(
		ds,
		datalayer.WithSchemaMode(datalayer.SchemaModeReadLegacyWriteBoth),
		datalayer.WithRelationshipVersioning(
			datalayer.RelationshipVersionV1,
			datalayer.RelationshipVersionV2,
			datalayer.RelationshipVersionV3,
		),
	)

	ctx := datalayer.ContextWithDataLayer(t.Context(), dl)
	dispatcher, err := graph.NewLocalOnlyDispatcher(graph.DispatcherParameters{
		ConcurrencyLimits: graph.SharedConcurrencyLimits(1),
		DispatchChunkSize: 10,
		TypeSet:           caveattypes.Default.TypeSet,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dispatcher.Close() })

	schemas := []string{
		`definition user {}
definition document {
	relation viewer: user
}`,
		`definition user {}
definition team {
	relation member: user
}
definition document {
	relation viewer: user
	relation indirect_viewer: team#member
}`,
		`definition user {}
definition team {
	relation member: user
}
definition document {
	relation viewer: user | team#member
	relation indirect_viewer: team#member
}`,
	}

	revisions := make([]datastore.Revision, 0, len(schemas))
	for _, schemaText := range schemas {
		definitions, text := compileRelationshipVersionSchema(t, schemaText)
		revision, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt datalayer.ReadWriteTransaction) error {
			if _, err := rwt.WriteSchema(ctx, definitions, text, caveattypes.Default.TypeSet); err != nil {
				return err
			}
			return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
				tuple.Touch(tuple.MustParse("document:doc1#viewer@user:tom")),
			})
		})
		require.NoError(t, err)
		revisions = append(revisions, revision)
	}

	for schemaIndex, revision := range revisions {
		for _, version := range datalayer.SupportedRelationshipVersions() {
			checkCtx := datalayer.ContextWithRelationshipVersion(ctx, version)
			result, _, err := computed.ComputeCheck(
				checkCtx,
				dispatcher,
				caveattypes.Default.TypeSet,
				computed.CheckParameters{
					ResourceType: tuple.RR("document", "viewer"),
					Subject:      tuple.ONR("user", "tom", tuple.Ellipsis),
					AtRevision:   revision,
					MaximumDepth: 20,
					SchemaHash:   datalayer.NoSchemaHashForTesting,
				},
				"doc1",
				10,
			)
			require.NoError(t, err)
			require.Equal(
				t,
				dispatchv1.ResourceCheckResult_MEMBER,
				result.Membership,
				"schema version %d, relationship version %s",
				schemaIndex+1,
				version,
			)
		}
	}
}
