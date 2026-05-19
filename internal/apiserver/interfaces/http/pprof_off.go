//go:build !pprof

package http

import "net/http"

func init() {
	registerPprofHooks = func(*http.ServeMux) {}
}
