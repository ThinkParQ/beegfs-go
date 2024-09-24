BeeGFS Control Tools and Libraries
==================================

`ctl` provides functionality for managing BeeGFS. It contains various "frontends" such as the
`beegfs` command-line tool that use shared "backend" functionality. The backend packages are also
setup to allow external use, and are likely what most developers interested in integrating their
applications with BeeGFS will want to leverage.

The codebase roughly adheres to the unofficial [standard Go project
layout](https://github.com/golang-standards/project-layout). Main applications intended to be built
into binaries can be found under `ctl/cmd/` and library code safe for use by external
applications can be found under `ctl/pkg/`. Internal functionality not suitable for external
reuse is under `ctl/internal`.
