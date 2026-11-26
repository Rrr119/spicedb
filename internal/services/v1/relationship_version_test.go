package v1_test

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestRelationshipVersionAPIMetadataCompatibility(t *testing.T) {
	config := testserver.DefaultTestServerConfig
	config.DataLayerOpts = append(config.DataLayerOpts, datalayer.WithRelationshipVersioning(
		datalayer.RelationshipVersionV1,
		datalayer.RelationshipVersionV2,
		datalayer.RelationshipVersionV3,
	))

	conn, _, _ := testserver.NewTestServerWithConfig(
		t,
		0,
		memdb.DisableGC,
		true,
		config,
		tf.StandardDatastoreWithData,
	)
	client := v1.NewPermissionsServiceClient(conn)

	writeResponse, err := client.WriteRelationships(t.Context(), &v1.WriteRelationshipsRequest{
		Updates: []*v1.RelationshipUpdate{
			tuple.MustUpdateToV1RelationshipUpdate(
				tuple.Touch(tuple.MustParse("document:versioned#viewer@user:tom")),
			),
		},
	})
	require.NoError(t, err)

	for _, version := range datalayer.SupportedRelationshipVersions() {
		ctx := metadata.AppendToOutgoingContext(
			t.Context(),
			datalayer.RelationshipVersionMetadataKey,
			string(version),
		)
		stream, err := client.ReadRelationships(ctx, &v1.ReadRelationshipsRequest{
			Consistency: &v1.Consistency{
				Requirement: &v1.Consistency_AtLeastAsFresh{
					AtLeastAsFresh: writeResponse.WrittenAt,
				},
			},
			RelationshipFilter: &v1.RelationshipFilter{
				ResourceType:       "document",
				OptionalResourceId: "versioned",
			},
		})
		require.NoError(t, err)

		response, err := stream.Recv()
		require.NoError(t, err)
		require.Equal(t, "document:versioned#viewer@user:tom", tuple.MustV1RelString(response.Relationship))
		_, err = stream.Recv()
		require.ErrorIs(t, err, io.EOF)
	}

	invalidCtx := metadata.AppendToOutgoingContext(
		t.Context(),
		datalayer.RelationshipVersionMetadataKey,
		"v99",
	)
	stream, err := client.ReadRelationships(invalidCtx, &v1.ReadRelationshipsRequest{
		RelationshipFilter: &v1.RelationshipFilter{ResourceType: "document"},
	})
	if err == nil {
		_, err = stream.Recv()
	}
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
