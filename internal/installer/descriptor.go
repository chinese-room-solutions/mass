package installer

import (
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// ServiceRoute describes a single RPC method exposed by a app.
type ServiceRoute struct {
	// FullMethod is the ConnectRPC-style path: "/{package}.{Service}/{Method}".
	FullMethod string
	// Method is the short method name (e.g. "Embed").
	Method string
}

// ParseServiceDescriptor reads a compiled FileDescriptorSet (.pb file) and
// extracts all service methods as ConnectRPC-convention routes.
// The returned routes use the format "/{package}.{service}/{method}".
func ParseServiceDescriptor(pbPath string) ([]ServiceRoute, error) {
	data, err := os.ReadFile(pbPath)
	if err != nil {
		return nil, fmt.Errorf("reading descriptor: %w", err)
	}

	var fds descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(data, &fds); err != nil {
		return nil, fmt.Errorf("parsing descriptor: %w", err)
	}

	var routes []ServiceRoute
	for _, fd := range fds.GetFile() {
		pkg := fd.GetPackage()
		for _, svc := range fd.GetService() {
			fqn := svc.GetName()
			if pkg != "" {
				fqn = pkg + "." + svc.GetName()
			}
			for _, m := range svc.GetMethod() {
				routes = append(routes, ServiceRoute{
					FullMethod: "/" + fqn + "/" + m.GetName(),
					Method:     m.GetName(),
				})
			}
		}
	}

	if len(routes) == 0 {
		return nil, fmt.Errorf("no services found in descriptor")
	}
	return routes, nil
}

// ParseServiceDescriptorFromDir reads a service.pb from a app directory
// using the path specified in app metadata.
func ParseServiceDescriptorFromDir(appDir string, meta *AppMetadata) ([]ServiceRoute, error) {
	if meta == nil || meta.ServiceProto == "" {
		return nil, nil
	}
	pbPath := filepath.Join(appDir, meta.ServiceProto)
	return ParseServiceDescriptor(pbPath)
}
