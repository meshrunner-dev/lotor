// meta prints the product identity for scripts and workflows, so the
// shell consumes the same Go source every binary does. Not shipped.
//
//	go run ./internal/product/cmd/meta -field slug
//	go run ./internal/product/cmd/meta -json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"meshrunner.dev/lotor/internal/product"
)

func main() {
	field := flag.String("field", "",
		"print one field: slug, name, description, homepage, update-base, binary, state-dir, service")
	asJSON := flag.Bool("json", false, "print every field as JSON")
	flag.Parse()

	fields := map[string]string{
		"slug":        product.Slug,
		"name":        product.Name,
		"description": product.Description,
		"homepage":    product.Homepage,
		"update-base": product.UpdateBase,
		"binary":      product.InstallBinary,
		"state-dir":   product.InstallStateDir,
		"service":     product.InstallService,
	}
	switch {
	case *asJSON:
		out, err := json.MarshalIndent(fields, "", "  ")
		if err != nil {
			fmt.Fprintln(os.Stderr, "meta:", err)
			os.Exit(1)
		}
		fmt.Println(string(out))
	case *field != "":
		v, ok := fields[*field]
		if !ok {
			fmt.Fprintf(os.Stderr, "meta: unknown field %q\n", *field)
			os.Exit(1)
		}
		fmt.Println(v)
	default:
		flag.Usage()
		os.Exit(2)
	}
}
