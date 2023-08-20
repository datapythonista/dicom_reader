# DICOM reader

**Unfinished and in early stage development.**

A reader of DICOM files to be used with Polars or pandas.

The reader loads `.dcm` files from a directory into an Apache Arrow
structure.

## Installation and first steps

To run this project it is required:

- Cargo for the Rust part
- [Pixi](https://prefix.dev) for the Python part

Once installed run:

```
$ cargo build
$ pixi run main
```
