package main

import (
	_ "embed"
	"net/http"
)

// page is the manual-test client (gateway/static/index.html), embedded so the
// gateway stays a single binary. Edit the HTML file, not this const.

//go:embed static/index.html
var page string

func servePage(w http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/" {
		http.NotFound(w, request)

		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(page))
}
