## Troubleshooting

### Iconv linker errors
When the `python` feature is enabled and you are getting iconv related linker errors on mac like this:
```
Undefined symbols for architecture arm64:
            "_iconv", referenced from:...
```

This is caused by multiple versions of iconv being linked into the application binary.
- Gdal links against the iconv from the mac SDK
- If py03 detects miniconda from homebrew it will link against that version of iconv causing the linker errors

Possible solutions:
- Don't invoke the build from a conda environment
- Specify the python version to use with the `PYO3_PYTHON` environment variable <br>
  e.g. `export PYO3_PYTHON=/opt/homebrew/bin/python3`

### pyarrow missing
To run the unit tests when the `python` feature is enabled `pyarrow` needs to be installed in the active python environment
`pip install pyarrow`