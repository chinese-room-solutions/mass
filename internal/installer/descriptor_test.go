package installer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestParseServiceDescriptor(t *testing.T) {
	// Build a minimal FileDescriptorSet with one service and two methods.
	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name:    strPtr("test.proto"),
				Package: strPtr("mass.module.test"),
				Service: []*descriptorpb.ServiceDescriptorProto{
					{
						Name: strPtr("TestService"),
						Method: []*descriptorpb.MethodDescriptorProto{
							{Name: strPtr("DoStuff")},
							{Name: strPtr("DoMore")},
						},
					},
				},
			},
		},
	}

	data, err := proto.Marshal(fds)
	require.NoError(t, err)

	dir := t.TempDir()
	pbPath := filepath.Join(dir, "service.pb")
	require.NoError(t, os.WriteFile(pbPath, data, 0644))

	routes, err := ParseServiceDescriptor(pbPath)
	require.NoError(t, err)
	require.Len(t, routes, 2)

	require.Equal(t, "/mass.module.test.TestService/DoStuff", routes[0].FullMethod)
	require.Equal(t, "DoStuff", routes[0].Method)

	require.Equal(t, "/mass.module.test.TestService/DoMore", routes[1].FullMethod)
	require.Equal(t, "DoMore", routes[1].Method)
}

func TestParseServiceDescriptor_NoPackage(t *testing.T) {
	fds := &descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{
			{
				Name: strPtr("test.proto"),
				Service: []*descriptorpb.ServiceDescriptorProto{
					{
						Name: strPtr("Svc"),
						Method: []*descriptorpb.MethodDescriptorProto{
							{Name: strPtr("Ping")},
						},
					},
				},
			},
		},
	}

	data, err := proto.Marshal(fds)
	require.NoError(t, err)

	dir := t.TempDir()
	pbPath := filepath.Join(dir, "service.pb")
	require.NoError(t, os.WriteFile(pbPath, data, 0644))

	routes, err := ParseServiceDescriptor(pbPath)
	require.NoError(t, err)
	require.Len(t, routes, 1)
	require.Equal(t, "/Svc/Ping", routes[0].FullMethod)
}

func TestParseServiceDescriptor_EmptyFile(t *testing.T) {
	fds := &descriptorpb.FileDescriptorSet{}
	data, err := proto.Marshal(fds)
	require.NoError(t, err)

	dir := t.TempDir()
	pbPath := filepath.Join(dir, "service.pb")
	require.NoError(t, os.WriteFile(pbPath, data, 0644))

	_, err = ParseServiceDescriptor(pbPath)
	require.ErrorContains(t, err, "no services found")
}

func TestParseServiceDescriptor_MissingFile(t *testing.T) {
	_, err := ParseServiceDescriptor("/nonexistent/service.pb")
	require.Error(t, err)
	require.ErrorContains(t, err, "reading descriptor")
}

func TestParseServiceDescriptorFromDir_NoMeta(t *testing.T) {
	routes, err := ParseServiceDescriptorFromDir("/some/dir", nil)
	require.NoError(t, err)
	require.Nil(t, routes)
}

func TestParseServiceDescriptorFromDir_EmptyServiceProto(t *testing.T) {
	routes, err := ParseServiceDescriptorFromDir("/some/dir", &ModuleMetadata{})
	require.NoError(t, err)
	require.Nil(t, routes)
}

func strPtr(s string) *string { return &s }
