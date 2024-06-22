package ocifs

import (
	"archive/tar"
	"os"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestUnifiedTreeStress(t *testing.T) {
	tests := []struct {
		name          string
		layers        [][]tar.Header
		expectedFiles []unifiedTreeNode
	}{
		{
			name: "Simple layering",
			layers: [][]tar.Header{
				{
					{Name: "file1.txt", Size: 100, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/file2.txt", Size: 200, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "file3.txt", Size: 300, ModTime: time.Now(), Mode: 0644},
				},
			},
			expectedFiles: []unifiedTreeNode{
				{name: "dir1/file2.txt", rootPath: "/layer1"},
				{name: "file1.txt", rootPath: "/layer1"},
				{name: "file3.txt", rootPath: "/layer2"},
			},
		},
		{
			name: "Whiteout file",
			layers: [][]tar.Header{
				{
					{Name: "file1.txt", Size: 100, ModTime: time.Now(), Mode: 0644},
					{Name: "file2.txt", Size: 200, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: ".wh.file1.txt", Size: 0, ModTime: time.Now(), Mode: 0644},
				},
			},
			expectedFiles: []unifiedTreeNode{
				{name: "file2.txt", rootPath: "/layer1"},
			},
		},
		{
			name: "Whiteout directory",
			layers: [][]tar.Header{
				{
					{Name: "dir1/file1.txt", Size: 100, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/file2.txt", Size: 200, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: ".wh.dir1", Size: 0, ModTime: time.Now(), Mode: 0644},
					{Name: "dir2/file3.txt", Size: 300, ModTime: time.Now(), Mode: 0644},
				},
			},
			expectedFiles: []unifiedTreeNode{
				{name: "dir2/file3.txt", rootPath: "/layer2"},
			},
		},
		{
			name: "Complex layering with whiteouts",
			layers: [][]tar.Header{
				{
					{Name: "file1.txt", Size: 100, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/file2.txt", Size: 200, ModTime: time.Now(), Mode: 0644},
					{Name: "dir2/file3.txt", Size: 300, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: ".wh.file1.txt", Size: 0, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/file4.txt", Size: 400, ModTime: time.Now(), Mode: 0644},
					{Name: ".wh.dir2", Size: 0, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "file5.txt", Size: 500, ModTime: time.Now(), Mode: 0644},
					{Name: "dir3/file6.txt", Size: 600, ModTime: time.Now(), Mode: 0644},
				},
			},
			expectedFiles: []unifiedTreeNode{
				{name: "dir1/file2.txt", rootPath: "/layer1"},
				{name: "dir1/file4.txt", rootPath: "/layer2"},
				{name: "dir3/file6.txt", rootPath: "/layer3"},
				{name: "file5.txt", rootPath: "/layer3"},
			},
		},
		{
			name: "Nested whiteouts",
			layers: [][]tar.Header{
				{
					{Name: "dir1/file1.txt", Size: 100, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/dir2/file2.txt", Size: 200, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/dir3/file3.txt", Size: 300, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "dir1/.wh.dir2", Size: 0, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/dir3/file4.txt", Size: 400, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "dir1/dir3/.wh.file3.txt", Size: 0, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/file5.txt", Size: 500, ModTime: time.Now(), Mode: 0644},
				},
			},
			expectedFiles: []unifiedTreeNode{
				{name: "dir1/dir3/file4.txt", rootPath: "/layer2"},
				{name: "dir1/file1.txt", rootPath: "/layer1"},
				{name: "dir1/file5.txt", rootPath: "/layer3"},
			},
		},
		{
			name: "File replacements",
			layers: [][]tar.Header{
				{
					{Name: "file1.txt", Size: 100, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/file2.txt", Size: 200, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "file1.txt", Size: 150, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/file3.txt", Size: 300, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "dir1/file2.txt", Size: 250, ModTime: time.Now(), Mode: 0644},
				},
			},
			expectedFiles: []unifiedTreeNode{
				{name: "dir1/file2.txt", rootPath: "/layer3"},
				{name: "dir1/file3.txt", rootPath: "/layer2"},
				{name: "file1.txt", rootPath: "/layer2"},
			},
		},
		{
			name: "Complex nested structure with whiteouts",
			layers: [][]tar.Header{
				{
					{Name: "dir1/file1.txt", Size: 100, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/dir2/file2.txt", Size: 200, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/dir2/dir3/file3.txt", Size: 300, ModTime: time.Now(), Mode: 0644},
					{Name: "dir4/file4.txt", Size: 400, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "dir1/.wh.dir2", Size: 0, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/newfile.txt", Size: 500, ModTime: time.Now(), Mode: 0644},
					{Name: "dir4/file4.txt", Size: 450, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "dir1/dir2/file2.txt", Size: 250, ModTime: time.Now(), Mode: 0644},
					{Name: ".wh.dir4", Size: 0, ModTime: time.Now(), Mode: 0644},
					{Name: "dir5/file5.txt", Size: 550, ModTime: time.Now(), Mode: 0644},
				},
			},
			expectedFiles: []unifiedTreeNode{
				{name: "dir1/dir2/file2.txt", rootPath: "/layer3"},
				{name: "dir1/file1.txt", rootPath: "/layer1"},
				{name: "dir1/newfile.txt", rootPath: "/layer2"},
				{name: "dir5/file5.txt", rootPath: "/layer3"},
			},
		},
		{
			name: "Whiteout entire directory then recreate",
			layers: [][]tar.Header{
				{
					{Name: "dir1/file1.txt", Size: 100, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/file2.txt", Size: 200, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: ".wh.dir1", Size: 0, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "dir1/file3.txt", Size: 300, ModTime: time.Now(), Mode: 0644},
				},
			},
			expectedFiles: []unifiedTreeNode{
				{name: "dir1/file3.txt", rootPath: "/layer3"},
			},
		},
		{
			name: "Complex file and directory replacements",
			layers: [][]tar.Header{
				{
					{Name: "dir1/file1.txt", Size: 100, ModTime: time.Now(), Mode: 0644},
					{Name: "dir2/file2.txt", Size: 200, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "dir1/.wh..wh..opq", Size: 0, ModTime: time.Now(), Mode: int64(os.ModeDir) | 0755},
					{Name: "dir2/.wh.file2.txt", Size: 0, ModTime: time.Now(), Mode: 0644},
					{Name: "dir2/file3.txt", Size: 300, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "dir1/newfile.txt", Size: 400, ModTime: time.Now(), Mode: 0644},
					{Name: "dir2/file2.txt", Size: 500, ModTime: time.Now(), Mode: 0644},
				},
			},
			expectedFiles: []unifiedTreeNode{
				{name: "dir1/newfile.txt", rootPath: "/layer3"},
				{name: "dir2/file2.txt", rootPath: "/layer3"},
				{name: "dir2/file3.txt", rootPath: "/layer2"},
			},
		},
		{
			name: "Nested whiteouts",
			layers: [][]tar.Header{
				{
					{Name: "dir1/file1.txt", Size: 100, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/dir2/file2.txt", Size: 200, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/dir3/file3.txt", Size: 300, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "dir1/.wh.dir2", Size: 0, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/dir3/file4.txt", Size: 400, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "dir1/dir3/.wh.file3.txt", Size: 0, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/file5.txt", Size: 500, ModTime: time.Now(), Mode: 0644},
				},
			},
			expectedFiles: []unifiedTreeNode{
				{name: "dir1/dir3/file4.txt", rootPath: "/layer2"},
				{name: "dir1/file1.txt", rootPath: "/layer1"},
				{name: "dir1/file5.txt", rootPath: "/layer3"},
			},
		},
		{
			name: "File replacements",
			layers: [][]tar.Header{
				{
					{Name: "file1.txt", Size: 100, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/file2.txt", Size: 200, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "file1.txt", Size: 150, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/file3.txt", Size: 300, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "dir1/file2.txt", Size: 250, ModTime: time.Now(), Mode: 0644},
				},
			},
			expectedFiles: []unifiedTreeNode{
				{name: "dir1/file2.txt", rootPath: "/layer3"},
				{name: "dir1/file3.txt", rootPath: "/layer2"},
				{name: "file1.txt", rootPath: "/layer2"},
			},
		},
		{
			name: "Complex nested structure with whiteouts",
			layers: [][]tar.Header{
				{
					{Name: "dir1/file1.txt", Size: 100, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/dir2/file2.txt", Size: 200, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/dir2/dir3/file3.txt", Size: 300, ModTime: time.Now(), Mode: 0644},
					{Name: "dir4/file4.txt", Size: 400, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "dir1/.wh.dir2", Size: 0, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/newfile.txt", Size: 500, ModTime: time.Now(), Mode: 0644},
					{Name: "dir4/file4.txt", Size: 450, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "dir1/dir2/file2.txt", Size: 250, ModTime: time.Now(), Mode: 0644},
					{Name: ".wh.dir4", Size: 0, ModTime: time.Now(), Mode: 0644},
					{Name: "dir5/file5.txt", Size: 550, ModTime: time.Now(), Mode: 0644},
				},
			},
			expectedFiles: []unifiedTreeNode{
				{name: "dir1/dir2/file2.txt", rootPath: "/layer3"},
				{name: "dir1/file1.txt", rootPath: "/layer1"},
				{name: "dir1/newfile.txt", rootPath: "/layer2"},
				{name: "dir5/file5.txt", rootPath: "/layer3"},
			},
		},
		{
			name: "Whiteout entire directory then recreate",
			layers: [][]tar.Header{
				{
					{Name: "dir1/file1.txt", Size: 100, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/file2.txt", Size: 200, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: ".wh.dir1", Size: 0, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "dir1/file3.txt", Size: 300, ModTime: time.Now(), Mode: 0644},
				},
			},
			expectedFiles: []unifiedTreeNode{
				{name: "dir1/file3.txt", rootPath: "/layer3"},
			},
		},
		{
			name: "Complex file and directory replacements",
			layers: [][]tar.Header{
				{
					{Name: "dir1/file1.txt", Size: 100, ModTime: time.Now(), Mode: 0644},
					{Name: "dir2/file2.txt", Size: 200, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "dir1/.wh..wh..opq", Size: 0, ModTime: time.Now(), Mode: int64(os.ModeDir) | 0755},
					{Name: "dir2/.wh.file2.txt", Size: 0, ModTime: time.Now(), Mode: 0644},
					{Name: "dir2/file3.txt", Size: 300, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "dir1/newfile.txt", Size: 400, ModTime: time.Now(), Mode: 0644},
					{Name: "dir2/file2.txt", Size: 500, ModTime: time.Now(), Mode: 0644},
				},
			},
			expectedFiles: []unifiedTreeNode{
				{name: "dir1/newfile.txt", rootPath: "/layer3"},
				{name: "dir2/file2.txt", rootPath: "/layer3"},
				{name: "dir2/file3.txt", rootPath: "/layer2"},
			},
		},
		{
			name: "Opaque whiteout with preserved files",
			layers: [][]tar.Header{
				{
					{Name: "dir1/file1.txt", Size: 100, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/subdir/file2.txt", Size: 200, ModTime: time.Now(), Mode: 0644},
				},
				{
					{Name: "dir1/.a", Size: 0, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/.wh..wh..opq", Size: 0, ModTime: time.Now(), Mode: 0644},
					{Name: "dir1/file3.txt", Size: 300, ModTime: time.Now(), Mode: 0644},
				},
			},
			expectedFiles: []unifiedTreeNode{
				{name: "dir1/.a", rootPath: "/layer2"},
				{name: "dir1/file3.txt", rootPath: "/layer2"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := newUnifiedTree()
			for i, layer := range tt.layers {
				headers := make([]*tar.Header, len(layer))
				for j := range layer {
					headers[j] = &layer[j]
				}
				tree.AddLayer("/layer"+strconv.Itoa(i+1), headers)
			}

			var result []unifiedTreeNode
			tree.Traverse(func(node *unifiedTreeNode, pathStr string) bool {
				if node.Header() != nil && !node.isWhiteout {
					result = append(result, unifiedTreeNode{
						name:     pathStr[1:], // Remove leading slash
						rootPath: node.rootPath,
					})
				}
				return true
			})

			// Sort expectedFiles to match Traverse output
			sort.Slice(tt.expectedFiles, func(i, j int) bool {
				iPath := path.Join(tt.expectedFiles[i].name)
				jPath := path.Join(tt.expectedFiles[j].name)
				return strings.Compare(iPath, jPath) < 0
			})

			if !reflect.DeepEqual(result, tt.expectedFiles) {
				t.Errorf("Unexpected result\nGot:\n%v\nWant:\n%v", result, tt.expectedFiles)
			}
		})
	}
}
