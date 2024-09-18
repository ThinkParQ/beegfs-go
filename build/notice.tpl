# NOTICE

This NOTICE file is provided in compliance with the licenses of the components used in this software.

## Project Information
**Copyright (C) 2024 ThinkParQ**

This software includes third-party code and libraries. Below are the attributions and notices for
third-party components included in this distribution, in accordance with their respective licenses.

## azillionmonkeys.com/qed/hash.html

* License: Paul Hsieh OLD BSD license
* Project URL: [azillionmonkeys.com/qed/hash.html](http://www.azillionmonkeys.com/qed/hash.html)

### Original License
```
Copyright (c) 2010, Paul Hsieh All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted
provided that the following conditions are met:

    Redistributions of source code must retain the above copyright notice, this list of
    conditions and the following disclaimer.

    Redistributions in binary form must reproduce the above copyright notice, this list of
    conditions and the following disclaimer in the documentation and/or other materials provided
    with the distribution.

    Neither my name, Paul Hsieh, nor the names of any other contributors to the code use may not
    be used to endorse or promote products derived from this software without specific prior
    written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```
{{ range . }}
## {{ .Name }}

* License: {{ .LicenseName }}
* Project URL: [{{ .Name }}](https://{{ .Name }})

### Original License
```
{{ .LicenseText }}
```
{{ end }}

---

If you have any questions regarding the licensing of the components, please
contact the respective authors. For questions or feedback related to this NOTICE
file, please contact ThinkParQ at info@thinkparq.com.

