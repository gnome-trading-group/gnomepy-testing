# gnomepy-testing

This package is used internally at GTG for testing.

## Releasing a new version

The GitHub Actions workflow will automatically run with a tag matching the 
pattern `v*.*.*` is released.

```commandline
poetry version patch  # or minor/major
git commit -m "Release new version"
git tag v$(poetry version -s)
git push origin main --tags
```