// Package main implements a lightweight doc coverage checker for Go packages.
//
// This tool scans all packages in the current module, identifies exported
// identifiers (types, functions, methods, constants, variables), and reports
// which ones lack Godoc comments. It exits with non-zero status if coverage
// is below 100%, making it suitable for CI/CD enforcement.
//
// Usage:
//
//	go run ./tools/cmd/doccheck
//	# or via Makefile:
//	make doc-coverage
package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"os"
	"strings"

	"golang.org/x/tools/go/packages"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		return fmt.Errorf("failed to load packages: %w", err)
	}

	var undocumented []string
	var total int
	for _, pkg := range pkgs {
		if strings.HasSuffix(pkg.PkgPath, "_test") {
			continue
		}
		for _, file := range pkg.Syntax {
			fset := pkg.Fset
			ast.Inspect(file, func(n ast.Node) bool {
				switch decl := n.(type) {
				case *ast.GenDecl:
					for _, spec := range decl.Specs {
						switch s := spec.(type) {
						case *ast.TypeSpec:
							if s.Name.IsExported() {
								total++
								if !hasDoc(decl.Doc, s.Doc) {
									pos := fset.Position(s.Pos())
									undocumented = append(undocumented, fmt.Sprintf("%s:%d: type %s", pos.Filename, pos.Line, s.Name.Name))
								}
							}
						case *ast.ValueSpec:
							for _, name := range s.Names {
								if name.IsExported() {
									total++
									if !hasDoc(decl.Doc, s.Doc) {
										pos := fset.Position(name.Pos())
										kind := kindOf(decl.Tok)
										undocumented = append(undocumented, fmt.Sprintf("%s:%d: %s %s", pos.Filename, pos.Line, kind, name.Name))
									}
								}
							}
						}
					}
				case *ast.FuncDecl:
					if decl.Name.IsExported() {
						total++
						if decl.Doc == nil || len(decl.Doc.List) == 0 {
							pos := fset.Position(decl.Pos())
							recv := ""
							if decl.Recv != nil && len(decl.Recv.List) > 0 {
								recv = fmt.Sprintf("(%s).", exprToString(decl.Recv.List[0].Type))
							}
							undocumented = append(undocumented, fmt.Sprintf("%s:%d: func %s%s", pos.Filename, pos.Line, recv, decl.Name.Name))
						}
					}
				}
				return true
			})
		}
	}

	documented := total - len(undocumented)
	cov := 0.0
	if total > 0 {
		cov = float64(documented) / float64(total) * 100.0
	}
	fmt.Printf("Doc Coverage: %.1f%% (%d/%d)\n", cov, documented, total)
	if len(undocumented) > 0 {
		fmt.Printf("\nUndocumented (%d):\n", len(undocumented))
		for _, u := range undocumented {
			fmt.Printf("  %s\n", u)
		}
		return fmt.Errorf("documentation coverage is below 100%%")
	}
	fmt.Println("\n✓ All exported identifiers are documented!")
	return nil
}

func hasDoc(a, b *ast.CommentGroup) bool {
	return (a != nil && len(a.List) > 0) || (b != nil && len(b.List) > 0)
}

func kindOf(tok token.Token) string {
	if tok == token.CONST {
		return "const"
	}
	return "var"
}

func exprToString(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return "*" + exprToString(t.X)
	case *ast.SelectorExpr:
		return exprToString(t.X) + "." + t.Sel.Name
	default:
		return "?"
	}
}
