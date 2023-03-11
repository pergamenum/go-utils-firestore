.PHONY: # Makes every target phony.

# make tidy while you work to catch issues.
tidy: _fix_go _check_go _test

# Check Go files.
_check_go:
	go vet ./...
	staticcheck ./...

# Fix Go files, these might alter the files.
_fix_go:
	go mod tidy
	go fmt ./...

_test:
	go test -timeout 1m -v ./...
