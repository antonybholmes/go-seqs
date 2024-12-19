module github.com/antonybholmes/go-tracks

go 1.23

replace github.com/antonybholmes/go-dna => ../go-dna

replace github.com/antonybholmes/go-sys => ../go-sys

replace github.com/antonybholmes/go-basemath => ../go-basemath

require github.com/rs/zerolog v1.33.0

require github.com/antonybholmes/go-basemath v0.0.0-20240825181410-a6174a39116c // indirect

require (
	github.com/antonybholmes/go-dna v0.0.0-20241007150544-1b58eb1162ce
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	golang.org/x/sys v0.28.0 // indirect
)
