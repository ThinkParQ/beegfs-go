Common
======

This is a library of common packages used when implementing projects in Go at ThinkParQ around the
BeeGFS file system. Provided functionality includes configuration management and logging, along with
various data structures and custom types. The goal of these packages is twofold:

(1) Provide a similar user experience across all Go projects in the BeeGFS ecosystem for common
functionality such as logging or configuration.

(2) Avoid duplicate implementations of the same functionality in different projects by providing a
central repository of proven functionality that can be easily imported and updated using `go mod`.
This prevents developers from needing to copy/paste essentially identical blocks of code in multiple
projects creating a maintainability nightmare whenever fixes/enhancements are made to one
implementation.

In short, if you're developing functionality outside the core business logic of your application, it
probably already exists or should be implemented here.
