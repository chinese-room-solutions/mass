package installer

import (
	"archive/tar"
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/KernelPryanic/ctxerr"
	massmodule "github.com/chinese-room-solutions/mass-module"
	"gopkg.in/yaml.v3"
)

// ModuleMetadata is the schema for module.yml inside a .mass package.
type ModuleMetadata struct {
	Name         string       `yaml:"name"`
	Version      string       `yaml:"version"`
	Description  string       `yaml:"description,omitempty"`
	SDKVersion   string       `yaml:"sdk_version"`
	Command      string       `yaml:"command"`
	UIPath       string       `yaml:"ui_path,omitempty"`       // root path the module's UI serves (e.g. "/"); empty = no UI
	Icon         string       `yaml:"icon,omitempty"`          // path to icon image (e.g. "icon.png")
	ServiceProto string       `yaml:"service_proto,omitempty"` // path to compiled service.pb (FileDescriptorSet)
	Dependencies []Dependency `yaml:"dependencies,omitempty"`
}

// Dependency declares a required module with a semver constraint.
type Dependency struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`          // semver constraint, e.g. ">=0.5.0", "^1.2.0"
	Source  string `yaml:"source,omitempty"` // provider source, e.g. "github:owner/repo"
}

// metadataFileNames lists accepted metadata filenames in priority order.
var metadataFileNames = []string{"module.yml", "module.yaml", "module.json"}

// archiveFormat represents the type of archive.
type archiveFormat int

const (
	formatZip archiveFormat = iota
	formatTar
)

// detectFormat sniffs the archive type by reading the first few bytes.
func detectFormat(path string) (archiveFormat, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close() //nolint:errcheck // read-only

	var buf [4]byte
	if _, err := io.ReadFull(f, buf[:]); err != nil {
		return 0, fmt.Errorf("reading header: %w", err)
	}
	// ZIP magic: PK\x03\x04
	if buf[0] == 'P' && buf[1] == 'K' && buf[2] == 0x03 && buf[3] == 0x04 {
		return formatZip, nil
	}
	return formatTar, nil
}

// ReadMetadataFromArchive opens a .mass archive (zip or tar) and reads module.yml.
func ReadMetadataFromArchive(archivePath string) (*ModuleMetadata, error) {
	format, err := detectFormat(archivePath)
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("detecting format: %w", err), map[string]any{"archive": archivePath})
	}
	if format == formatTar {
		return readMetadataFromTar(archivePath)
	}
	return readMetadataFromZip(archivePath)
}

func readMetadataFromZip(archivePath string) (*ModuleMetadata, error) {
	r, err := zip.OpenReader(archivePath)
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("opening archive: %w", err), map[string]any{"archive": archivePath})
	}
	defer r.Close() //nolint:errcheck // read-only close

	for _, candidate := range metadataFileNames {
		for _, f := range r.File {
			base := filepath.Base(f.Name)
			if base == candidate {
				rc, err := f.Open()
				if err != nil {
					return nil, ctxerr.With(fmt.Errorf("opening %s: %w", candidate, err), map[string]any{"archive": archivePath})
				}
				defer rc.Close() //nolint:errcheck // read-only close

				data, err := io.ReadAll(rc)
				if err != nil {
					return nil, ctxerr.With(fmt.Errorf("reading %s: %w", candidate, err), map[string]any{"archive": archivePath})
				}

				var meta ModuleMetadata
				if err := yaml.Unmarshal(data, &meta); err != nil {
					return nil, ctxerr.With(fmt.Errorf("parsing %s: %w", candidate, err), map[string]any{"archive": archivePath})
				}
				return &meta, nil
			}
		}
	}

	return nil, ctxerr.With(fmt.Errorf("module.yml not found in archive"), map[string]any{"archive": archivePath})
}

func readMetadataFromTar(archivePath string) (*ModuleMetadata, error) {
	f, err := os.Open(archivePath)
	if err != nil {
		return nil, ctxerr.With(fmt.Errorf("opening archive: %w", err), map[string]any{"archive": archivePath})
	}
	defer f.Close() //nolint:errcheck // read-only close

	tr := tar.NewReader(f)
	candidates := make(map[string]bool, len(metadataFileNames))
	for _, c := range metadataFileNames {
		candidates[c] = true
	}

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, ctxerr.With(fmt.Errorf("reading tar: %w", err), map[string]any{"archive": archivePath})
		}
		base := filepath.Base(hdr.Name)
		if !candidates[base] {
			continue
		}

		data, err := io.ReadAll(tr)
		if err != nil {
			return nil, ctxerr.With(fmt.Errorf("reading %s: %w", base, err), map[string]any{"archive": archivePath})
		}
		var meta ModuleMetadata
		if err := yaml.Unmarshal(data, &meta); err != nil {
			return nil, ctxerr.With(fmt.Errorf("parsing %s: %w", base, err), map[string]any{"archive": archivePath})
		}
		return &meta, nil
	}

	return nil, ctxerr.With(fmt.Errorf("module.yml not found in archive"), map[string]any{"archive": archivePath})
}

// ValidateMetadata checks that the metadata is complete and compatible.
func ValidateMetadata(meta *ModuleMetadata) error {
	if meta.Name == "" {
		return fmt.Errorf("module name is required")
	}
	if meta.Version == "" {
		return fmt.Errorf("module version is required")
	}
	if meta.Command == "" {
		return fmt.Errorf("command is required")
	}
	if meta.SDKVersion == "" {
		return fmt.Errorf("sdk_version is required")
	}

	// Check SDK compatibility.
	expected := strconv.Itoa(int(massmodule.Handshake.ProtocolVersion))
	if meta.SDKVersion != expected {
		return fmt.Errorf("module requires SDK version %s, but MASS supports version %s", meta.SDKVersion, expected)
	}

	return nil
}

// ExecutableExistsInArchive checks that the command's executable exists in the archive.
// Only checks the first word of the command (the executable itself); additional
// arguments (e.g. script paths) are not validated here.
func ExecutableExistsInArchive(archivePath string, command string) error {
	if command == "" {
		return fmt.Errorf("command is empty")
	}
	format, err := detectFormat(archivePath)
	if err != nil {
		return ctxerr.With(fmt.Errorf("detecting format: %w", err), map[string]any{"archive": archivePath})
	}
	if format == formatTar {
		return execExistsInTar(archivePath, command)
	}
	return execExistsInZip(archivePath, command)
}

func execExistsInZip(archivePath string, command string) error {
	parts := strings.Fields(command)
	execName := filepath.Base(parts[0])

	r, err := zip.OpenReader(archivePath)
	if err != nil {
		return ctxerr.With(fmt.Errorf("opening archive: %w", err), map[string]any{"archive": archivePath})
	}
	defer r.Close() //nolint:errcheck // read-only close

	for _, f := range r.File {
		name := filepath.Base(f.Name)
		if name == execName && !f.FileInfo().IsDir() {
			return nil
		}
	}

	return ctxerr.With(fmt.Errorf("executable %q not found in archive", execName), map[string]any{"archive": archivePath, "executable": execName})
}

func execExistsInTar(archivePath string, command string) error {
	parts := strings.Fields(command)
	execName := filepath.Base(parts[0])

	f, err := os.Open(archivePath)
	if err != nil {
		return ctxerr.With(fmt.Errorf("opening archive: %w", err), map[string]any{"archive": archivePath})
	}
	defer f.Close() //nolint:errcheck // read-only close

	tr := tar.NewReader(f)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return ctxerr.With(fmt.Errorf("reading tar: %w", err), map[string]any{"archive": archivePath})
		}
		name := filepath.Base(hdr.Name)
		if name == execName && hdr.Typeflag != tar.TypeDir {
			return nil
		}
	}

	return ctxerr.With(fmt.Errorf("executable %q not found in archive", execName), map[string]any{"archive": archivePath, "executable": execName})
}

// ReadMetadataFromDir reads module.yml from an installed module directory.
func ReadMetadataFromDir(dir string) (*ModuleMetadata, error) {
	for _, candidate := range metadataFileNames {
		path := filepath.Join(dir, candidate)
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var meta ModuleMetadata
		if err := yaml.Unmarshal(data, &meta); err != nil {
			continue
		}
		return &meta, nil
	}
	return nil, ctxerr.With(fmt.Errorf("no module metadata found in %s", dir), map[string]any{"dir": dir})
}
