package ocifs

import (
	"archive/tar"
	"path"
	"sort"
	"strings"
)

type unifiedTreeNode struct {
	header         *tar.Header
	children       map[string]*unifiedTreeNode
	name           string
	rootPath       string
	isWhiteout     bool
	opaqueWhiteout bool
}

func (n *unifiedTreeNode) Path() string {
	return path.Join(n.rootPath, n.header.Name)
}

func (n *unifiedTreeNode) Header() *tar.Header {
	return n.header
}

type unifiedTree struct {
	root *unifiedTreeNode
}

func newUnifiedTree() *unifiedTree {
	return &unifiedTree{
		root: &unifiedTreeNode{
			name:     "/",
			children: make(map[string]*unifiedTreeNode),
		},
	}
}

func (fs *unifiedTree) AddLayer(rootPath string, files []*tar.Header) {
	for _, header := range files {
		fs.addFile(rootPath, header)
	}
}

func (fs *unifiedTree) addFile(rootPath string, header *tar.Header) {
	name := strings.Trim(header.Name, "/")
	if name == "." || name == "" {
		// This is a root entry, update the root node
		fs.root.header = header
		fs.root.rootPath = rootPath
		return
	}

	parts := strings.Split(name, "/")
	current := fs.root

	for i, part := range parts {
		if part == ".wh..wh..opq" {
			// Opaque whiteout: remove all children from lower layers
			for k, v := range current.children {
				if v.rootPath != rootPath {
					delete(current.children, k)
				}
			}
			current.opaqueWhiteout = true
			return
		}

		if strings.HasPrefix(part, ".wh.") {
			// Handle regular whiteout
			realName := strings.TrimPrefix(part, ".wh.")
			if i == len(parts)-1 {
				// Whiteout file
				delete(current.children, realName)
				current.children[part] = &unifiedTreeNode{name: part, header: header, isWhiteout: true, rootPath: rootPath}
			} else {
				// Whiteout directory
				delete(current.children, realName)
			}
			return
		}

		if next, exists := current.children[part]; exists {
			current = next
		} else {
			newNode := &unifiedTreeNode{
				name:     part,
				children: make(map[string]*unifiedTreeNode),
				rootPath: rootPath,
			}
			current.children[part] = newNode
			current = newNode
		}
	}

	// Update the node, including its rootPath
	current.header = header
	current.rootPath = rootPath
}

func (fs *unifiedTree) removeSubtree(parent *unifiedTreeNode, name string) {
	delete(parent.children, name)
	whiteoutNode := &unifiedTreeNode{
		name:       ".wh." + name,
		isWhiteout: true,
		children:   make(map[string]*unifiedTreeNode),
	}
	parent.children[".wh."+name] = whiteoutNode
}

func (fs *unifiedTree) getNode(pathStr string) *unifiedTreeNode {
	if pathStr == "/" || pathStr == "" {
		return fs.root
	}

	parts := strings.Split(strings.Trim(pathStr, "/"), "/")
	current := fs.root

	for _, part := range parts {
		if next, exists := current.children[part]; exists {
			current = next
		} else {
			return nil
		}
	}

	return current
}

func (fs *unifiedTree) Traverse(callback func(*unifiedTreeNode, string) bool) {
	if fs.root == nil {
		return
	}
	fs.traverseDFS(fs.root, "", callback)
}

func (fs *unifiedTree) traverseDFS(node *unifiedTreeNode, pathStr string, callback func(*unifiedTreeNode, string) bool) bool {
	if node.isWhiteout {
		return true
	}

	fullPath := path.Join(pathStr, node.name)
	if node.header != nil {
		if !callback(node, fullPath) {
			return false
		}
	}

	children := make([]*unifiedTreeNode, 0, len(node.children))
	for _, child := range node.children {
		children = append(children, child)
	}

	sort.Slice(children, func(i, j int) bool {
		return children[i].name < children[j].name
	})

	for _, child := range children {
		if !fs.traverseDFS(child, fullPath, callback) {
			return false
		}
	}

	return true
}

func (fs *unifiedTree) Get(pathStr string) (*unifiedTreeNode, bool) {
	// Normalize the path
	pathStr = path.Clean("/" + pathStr)

	// Handle root case
	if pathStr == "/" {
		return fs.root, true
	}

	// Split the path into parts
	parts := strings.Split(strings.TrimPrefix(pathStr, "/"), "/")

	current := fs.root
	for _, part := range parts {
		next, exists := current.children[part]
		if !exists {
			return nil, false
		}
		if next.isWhiteout {
			return nil, false
		}
		current = next
	}

	return current, true
}
