# The GO_BUILD_VERSION is the official go version the project is built/tested
# with. When updating be sure to also change any .github/workflows/ that install
# a specific Go version and update the go.mod file.
GO_BUILD_VERSION="go1.20.7"
INSTALLED_VERSION=$(go version | { read _ _ ver _; echo ${ver}; } )  || die "determining version of go failed"

if [ "$INSTALLED_VERSION" == "$GO_BUILD_VERSION" ]
then
  echo "The installed go version (${INSTALLED_VERSION}) matches the expected build go version (${GO_BUILD_VERSION})."
  
else
  echo "WARNING - we expect to build with go version ${GO_BUILD_VERSION} but found go version ${INSTALLED_VERSION}"
  exit 1
fi