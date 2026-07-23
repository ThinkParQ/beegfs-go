# `beegfs` CLI Style Guide

## Background

This guide codifies the conventions the `beegfs` CLI follows, so they can be applied and enforced
consistently. This guide originated in July 2026 from an effort to polish the `beegfs` CLI's help
text, flags, output, and errors (the "Polish CTL help text, flags, output, and errors" issue). We
surveyed the conventions the `ctl` codebase had already converged on, codified them here — giving
documented standards precedence over merely implied ones — and then applied the guide across the CLI
in a series of reviewed commits. A few items were consciously left as follow-ups; they carry a
**Deferred:** note below. This document is the lasting outcome and is meant to stand on its own as
the reference for new CLI work.

## Scope

This governs the user-facing surface of the `beegfs` command line tool (`ctl/cmd/beegfs`, wired in
`ctl/internal/cmd/`) and the backend library it drives (`ctl/pkg/ctl/`): **output streams, structured
output, errors, exit codes, command/flag definitions, help text, and message tone.**

It is a CLI-specific *supplement* to the project-wide Go coding standards linked from
[`CONTRIBUTING.md`](../CONTRIBUTING.md) (the ThinkParQ wiki). Where this guide and the wiki overlap
(e.g. Go error conventions), the wiki is authoritative and this guide only restates the parts that
shape CLI behavior.

## How to read this guide (provenance)

Every rule is tagged with where it comes from. **When two rules would conflict, a `[Documented]`
rule always wins over a `[Practice]` rule** — that is the core principle of this guide.

| Tag | Meaning |
|-----|---------|
| **`[Documented]`** | An explicit, existing standard — a docstring, code comment, README, `CLAUDE.md`, or a language convention. Authoritative. Non-negotiable unless the documented source is deliberately changed. Source is cited. |
| **`[Practice]`** | An unwritten convention the code has converged on (a clear majority). Codified here for the first time; adopt it and treat the minority as cleanup. The evidence (approx. share / examples) is cited. |
| **`[Decision needed]`** | Genuinely split practice with no documented standard. A recommendation is given, but it must be **ratified during review** before it becomes a rule. |

A rule may carry a **Deferred:** note — a known gap intentionally left to a follow-up; everything
else has been brought into line.

---

## 1. Architecture: who prints

- **`[Documented]` Backend (`ctl/pkg/ctl/…`) returns data and errors; it must not print or exit.**
  It is an importable library ([`ctl/README.md`](README.md), [`../CLAUDE.md`](../CLAUDE.md)). All
  user-facing output and all process exit decisions belong to the frontend (`ctl/internal/cmd/…`).
  - *Today this is honored:* `pkg/ctl/` has zero direct writes to stdout/stderr. Preserve it.
- **`[Documented]` The frontend prints; only `main.go`/`Execute()` decides the exit code.**
  Commands return an `error` (optionally a `util.CtlError` carrying an exit code); `Execute()` in
  [`root.go`](internal/cmd/root.go) prints it and exits. Commands must not call `os.Exit`.

## 2. Output streams (stdout vs stderr)

- **`[Documented]` Structured output that a script might parse goes to stdout.** Tables and JSON are
  rendered by `cmdfmt.Printomatic`, which writes to stdout
  ([`cmdfmt/fmt.go`](internal/cmdfmt/fmt.go)).
- **`[Documented]` Everything else — info, status, progress, warnings, hints — goes to stderr, via
  `cmdfmt.Printf`.** This is the stated purpose of `cmdfmt.Printf`
  ([`cmdfmt/fmt.go:14-18`](internal/cmdfmt/fmt.go#L14)): *"like `fmt.Printf` except it prints to
  stderr … intended to be used from commands that print structured output … that may be parsed by
  scripts."*
- **`[Documented]` Errors go to stderr** with an `Error: ` prefix, centrally, in `Execute()`
  ([`root.go`](internal/cmd/root.go)). Commands surface errors by returning them, not by printing
  them.
- **`[Documented]` A per-item *result* failure is result content, not an operational error.** When a
  command reports an outcome per item (per node, per file, per target), a failed item belongs on
  **stdout** with the other results — it is that item's result, the counterpart to its success line —
  not on stderr. Reserve stderr for *operational* messages about running the command itself (couldn't
  connect, couldn't read config, progress, "skipping…"). A command's *overall* failure is surfaced by
  **returning** an error (→ `Execute()` → stderr), never by printing it as report content. *(Example:
  `node ping` prints each node's outcome — success or "Error pinging …" — on stdout, and returns a
  `CtlError` for the run's overall failure.)*
- **`[Practice]` Choose the output stream deliberately.** `cmdfmt.Printf` → **stderr** for status,
  info, warnings, progress, and hints. `Printomatic` → **stdout** for tabular/row data (it also
  gives `--output` JSON for free). Raw `fmt.Print*` writes to **stdout** — use it only for a
  command's intended human-readable *result* output that isn't row-oriented, and when you do you
  **must** also provide a structured JSON representation for `--output` (see §3). There is
  intentionally no `cmdfmt` stdout helper for free-form output — use `fmt` directly for that. The
  mistake to avoid is `fmt.Print*` that leaks status or errors onto stdout, not `fmt` itself; the
  backend (`pkg/ctl/`) must never print at all (§1).

> Litmus test: with `--output=json`, `beegfs <cmd> >out.json` leaves **only** parseable JSON in
> `out.json`; status, progress, warnings, and errors always go to the terminal (stderr) regardless of
> `--output`.

## 3. Structured output (tables & JSON)

- **`[Documented]` Use `cmdfmt.Printomatic` for all tabular/row-oriented output.** It gives every
  command `--output table|json|json-pretty|ndjson`, `--columns`, `--page-size`, and `--sort` for
  free ([`cmdfmt/fmt.go`](internal/cmdfmt/fmt.go), [`pkg/config`](pkg/config/config.go)). Do not
  hand-roll row output with `fmt` — doing so silently drops `--output` support.
- **`[Documented]` For *non-tabular* structured output (nested reports, not rows), use a single data
  model rendered two ways rather than `Printomatic`.** Collect the result into one value (JSON tags,
  and a `MarshalJSON` on any status enum so it serializes to a stable string, not an int), then
  render *that value* as either human text or `json.Marshal` output — so the two representations
  cannot drift. `health check` is the reference: [`pkg/ctl/health`](pkg/ctl/health) `Collect` returns
  a `Report`, and the frontend renders human text or JSON from it. Data the human renderer needs but
  that isn't part of the JSON contract (e.g. a raw type reused by an existing table printer) goes in a
  `json:"-"` field alongside the serialized projection, both built from the same collected data. Also
  route the command's operational messages to stderr as part of this work (§2), which is mandatory
  once stdout may carry JSON.
  - As of July 2026 all commands now honor `--output` including `health check`/`capacity`/`network`,
    `license`, `buddygroup resync stats`, and `benchmark`. New commands that emit reports must do
    the same from the start (rows → `Printomatic`; nested reports → the single-model pattern above),
    routing all non-data lines to stderr. Because raw `fmt` output to stdout has no automatic JSON
    (unlike `Printomatic`), any command that prints human results with `fmt` **owns** its `--output
    json` path — model the data once and render both representations from it.
- **`[Documented]` Column names are lowercase**, and spaces are written as underscores
  ([`cmdfmt/fmt.go:61`](internal/cmdfmt/fmt.go#L61) and the header-building code). Column identifiers
  double as JSON keys, so keep them stable, lowercase, and `snake_case`.

## 4. Errors

Go's error conventions are the baseline (see the coding-standards wiki). The CLI adds a few rules
because `Execute()` post-processes every returned error.

- **`[Documented]` Error strings are lowercase fragments with no trailing punctuation, wrapped with
  `%w`.** (Go convention) Proper nouns/acronyms (`BeeGFS`, `RST`, `SQL`) stay capitalized.
- **`[Documented]` Never start an error string with `error`, and never *nest* a `failed to`/`unable
  to`/`error` fragment under another.** `Execute()` already prepends `Error: `
  ([`root.go`](internal/cmd/root.go)), so `fmt.Errorf("error contacting node: …")` renders as
  `Error: error contacting node: …`. A single leading `failed to`/`unable to` at the *top level* is
  fine — e.g. `Error: unable to connect to management` — the problem is *stacking* them, e.g.
  `Error: unable to connect to management node: unable to auto-configure the address`. When wrapping,
  the inner fragment must not repeat the outer's `error`/`failed to`/`unable to`.
  - **Deferred:** the `ctl` `error `-prefixed strings were fixed; the `common/beemsg` sentinels
    `ErrBeeMsgWrite = "error writing BeeMsg"` / `ErrBeeMsgRead = "error reading BeeMsg"` remain
    (a cross-repo follow-up).
- **`[Practice]` Wrap only to add information the inner error doesn't already carry; otherwise
  `return err`.** If the outer text restates the transport/target the inner error already names,
  drop it.
  - **Deferred:** the 3-layer BeeMsg chain (`comm.go` "no response from any address" +
    `nodestore.go` "TCP request to X failed" + caller "…from node") should collapse so exactly one
    layer owns the "request to node X failed" phrasing (a cross-repo follow-up).
- **`[Practice]` Errors from shared helpers should be meaningful at the source; callers add only the
  operation.** `config.ManagementClient()`, `config.NodeStore()`, and `config.BeeGFSClient()`
  already return specific messages (auth file, cert, auto-config). Callers should usually
  `return err`, or add just the operation (`"refreshing entry info: %w"`) — **not** re-describe it as
  a connection failure.
- **`[Documented]` Use `%w`, never `%v`, for an error operand** so `errors.Is`/`errors.As` keep
  working. Never build an error without wrapping the cause when one exists.

## 5. Exit codes

- **`[Documented]` Use the named `util.CtlExitCode` constants; never raw integers.** Defined in
  [`util/error.go`](internal/util/error.go) and surfaced in the root help "Exit Codes" section:
  `Success` = 0, `GeneralError` = 1, `PartialSuccess` = 2. Return non-zero/partial results via
  `util.NewCtlError(err, code)`. Any code beyond 0/1/2 must be added to the enum, given a `String()`,
  and documented in the root help before use (`NodesUnreachable = 5` is the current example).

## 6. Commands

### 6.1 `Short`

- **`[Practice]` Imperative mood, capitalized first word, no trailing period.** (~78% already
  followed this at codification, e.g. `"List BeeGFS targets"`, `"Create a storage pool"`.)
  Parent/group commands use `"Manage …"`.
  - ✅ `"List buddy groups"`  ❌ `"Lists the buddy groups."`

### 6.2 `Long`

- **`[Documented]` Author `Long` (and `Example`) using the markdown-like help format** (see §9).
- **`[Practice]` `Long` is one or more full sentences**, expanding on `Short` and describing
  behavior, defaults, and caveats. Destructive commands state the risk explicitly in `Long`.

### 6.3 `Example`

- **`[Documented]` Every non-trivial command has an `Example:` block.**
  - **Format:** an `Example: ` label followed by a capitalized description with no trailing
    punctuation, then a **blank line**, then the command indented two spaces with a bare `beegfs`
    prefix (no `$`):
    ```
    Example: List all targets in a specific pool

      beegfs target list --pool 2
    ```
    The blank line is **required**. The help renderer (`getParagraphsForHelp` in
    [`root.go`](internal/cmd/root.go)) collapses a description and command with no blank line between
    them onto one line; a blank line makes the command its own indented paragraph, which the renderer
    preserves verbatim. `index/` follows this format. Repeat the `Example: … <blank> command` unit for
    each example.

### 6.4 Aliases, groups, `Use`

- **`[Practice]` The verb in `Short` must match the command's `Use` name** (e.g. `remote
  cleanup-orphaned` → "Clean up …"; see §8, delete vs clean up).
- **`[Documented]` Commands are organized with Cobra groups** (`GroupID`/`AddGroup`) in the help
  template ([`root.go`](internal/cmd/root.go)); new top-level commands should be assigned a group.

### 6.5 Authorization & health annotations

- **`[Documented]` Commands are root-only by default; opt out with the
  `authorization.AllowAllUsers` annotation** for read-only commands safe for any user
  ([`root.go` `isCommandAuthorized`](internal/cmd/root.go)). Non-root users must interact through a
  mount point.
- **`[Documented]` Set the `health.SkipAlerts` annotation** on commands where trailing health alerts
  would be redundant or require no remote communication (e.g. `version`, `license`)
  ([`root.go` `globalPersistentPostRunE`](internal/cmd/root.go)).

## 7. Flags

- **`[Documented]` Flag names are lowercase and hyphenated (`kebab-case`); matching is
  case-insensitive** ([`root.go` normalization](internal/cmd/root.go#L86)). Long flags are the
  norm; add a short flag only when it's genuinely common.
- **`[Documented]` Every flag is settable via env var `BEEGFS_<FLAG>`** — the flag name uppercased
  with hyphens replaced by underscores (root help template). Keep flag names env-var-friendly.
- **`[Practice]` Flag descriptions are capitalized and end with a period.** (~92% already followed
  this at codification.) This is the **opposite** of the `Short` convention — that's intentional;
  keep them distinct.
  - ✅ `"Filter by storage pool."`
- **`[Practice]` Use the shared BeeGFS pflag types** ([`common/beegfs`](../common/beegfs)) for
  BeeGFS-specific values (node types, entity IDs, etc.) so parsing and error messages stay
  consistent, and standard `pflag` otherwise. **`bflag`** ([`internal/bflag`](internal/bflag)) is
  only for commands that wrap an external CLI utility (e.g. `copy`) and need to mirror that tool's
  flag surface — don't reach for it otherwise.

### 7.1 Hidden and dangerous flags/commands

A flag or command is hidden for one of these legitimate reasons — and only these. Add a comment
stating which reason applies:

- **`[Practice]` Internal / developer-only** — diagnostics and dev affordances not meant for end
  users (e.g. `--log-developer`, `--pprof`, `entry set --data-state`).
- **`[Practice]` Support-only (by design)** — safe to share case-by-case with support but not
  surfaced to general users. Keeping these hidden is intentional; the only review question per flag
  is whether it's shared so routinely — and is safe enough — that it should simply be unhidden,
  rewording the name/description first if that's what kept it hidden.
- **`[Practice]` Dangerous-without-guidance** — stays hidden, documents the risk in `Long`, and is
  guarded by `--yes` (e.g. `buddygroup delete`, `target set-state`, `debug`).

There are **no truly deprecated flags** today, and nothing is hidden merely to phase it out. If a
flag/command ever does become obsolete, use Cobra's `Deprecated`/`MarkDeprecated` (visible and
dated) rather than silently hiding or removing it.

- **Open question (not blocking):** support-only flags worth *considering* for unhiding — the
  `node list --reachability-*` trio, the RST S3 flags (`--metadata`/`--tagging`/`--storage-class`),
  and `--use-http-proxy`.

## 8. Messages: tone & phrasing

### 8.1 Success / confirmation

- **`[Practice]` Object lifecycle results use `"<Noun> <past-tense>: <id>"`.** Uniform today across
  create/delete commands: `"Buddy group created: 3"`, `"Pool deleted: 2"`.
- **`[Practice]` Action results use `"Successfully <verb-ed> …"`.** Retire the `"Success: …"` prefix
  (RST commands) and one-offs like `"Migration complete."`.
- All confirmations go to **stderr** via `cmdfmt.Printf` (§2).

### 8.2 Warnings

- **`[Practice]` Prefix warnings with `WARNING: ` (all caps, colon, space)**, followed by a normal
  sentence. Uniform today; keep it.

### 8.3 Notes

- **`[Documented]` Prefix advisory notes with `Note: ` (capital N, colon)** — not `NOTE:`, `INFO:`,
  bare `Note that…`, or parenthetical `(note …)`.

### 8.4 Progress / status

- **`[Practice]` Progress and status lines go to stderr** via `cmdfmt.Printf` and read as plain
  sentences (`"Starting storage benchmark…"`). Never interleave them into stdout.

## 9. Help-text authoring format

`Long` and flag descriptions support a markdown-like source format that is re-wrapped to the
terminal width at render time (min 80 / max 150 cols). Rules — all **`[Documented]`** in
[`root.go`](internal/cmd/root.go) (`getParagraphsForHelp`, `mergeBulletsForHelp`, `wrapFlagUsages`,
`wrapHelpText`):

- **Blank line = paragraph break.** Within a paragraph, single line breaks are collapsed to spaces —
  so you can wrap source lines for readability without affecting output.
- **Indented lines are preserved verbatim** — use leading spaces for code/command/example blocks.
- **Bullet lists** (`- `, `* `, `•`, `1.`) are preserved with a newline between bullets; wrap a
  bullet across source lines freely.
- **A line ending in `\` forces a line break** at that point.
- **Tabs are stripped** from flag usage — indent continuation lines with tabs for source
  readability.
- **Header blocks:** a line ending in `:` starts a labeled block.

## 10. Terminology

**`[Documented]`** — canonical terms, ratified in review. Normalize all *user-facing* strings (help,
messages, errors) to the left column. Code identifiers are out of scope.

**Deliberate divergence from the user docs:** the `beegfs-core` user docs predominantly use
"service"/"server" (e.g. "management service" 82×, "metadata server" 104×) and two-word "file
system" (227×). The CLI intentionally standardizes on **node**-first terminology and one-word
**filesystem** — because `node` is the CLI's own first-class concept (`beegfs node list`, node
types/states) and for internal consistency. This CLI↔docs mismatch is a conscious, reviewed choice,
**not** an oversight — do not "reconcile" it by reverting the CLI toward the docs' wording. (Only the
RST/"BeeGFS Remote" service naming below is shared with the docs.)

| Use | Not | Notes |
|-----|-----|-------|
| **node** (`metadata node`, `storage node`, `management node`) | "server", "daemon" in user text | `stats` keeps "server" (matches its command name); the health "Connections to Server Nodes" title is intentionally left (also a JSON-feeding constant). |
| **management node** (`mgmtd` only in flag/addr contexts) | "management service", "the management" | |
| **Remote Storage Target (RST)** for the *feature* — spell out on first use, `RST` after; casual **remote target(s)** is fine (parallel to metadata/storage target). **BeeGFS Remote** for the *service* (`beegfs-remote.service`), when referring to the service itself. | "BeeRemote" (old one-word spelling) | Command stays `remote` with aliases `rst`, `remote-storage-target`. |
| **buddy group** (prose); `mirror` only as the command name | "buddygroup" (except CLI arg placeholders), "storage mirror", "buddy mirror group" | Align health section titles ("Metadata Mirrors" vs "Metadata Nodes"). |
| **delete** (permanent removal) / **clean up** (verb, for transient artifacts) | mixing delete/remove/cleanup/dispose for the same concept | "cleanup" is the noun; the verb is "clean up". |
| **I/O** | "IO" | |
| **filesystem** (one word) | "file system" (two words) | CLI standard is "filesystem" (code majority). Intentionally differs from the user docs/`CLAUDE.md` (which use "file system") — see the divergence note above. |

---

## Quick checklist (for code reviews)

- [ ] Deliberate stream choice: status/info/warnings → stderr (`cmdfmt.Printf`); rows → stdout (`Printomatic`); free-form human results → stdout (`fmt`) only with a matching `--output json` path.
- [ ] Backend (`pkg/ctl`) returns data/errors — no printing, no `os.Exit`.
- [ ] Error strings: lowercase fragment, no trailing punctuation, no leading `error`/`failed to`/`unable to`, wrapped with `%w`.
- [ ] Wrapping adds new info; otherwise `return err`. No restating transport/target a lower layer already named.
- [ ] Exit codes via named `util.CtlExitCode` constants only.
- [ ] `Short`: imperative, capitalized, **no** period. Flag descriptions: capitalized, **with** period.
- [ ] `Example:` present (recommended format) for non-trivial/destructive commands.
- [ ] `Use` verb matches `Short` verb. Root-only by default; `authorization.AllowAllUsers` for safe read-only commands.
- [ ] Flags: `kebab-case`, env-var-friendly; shared BeeGFS pflag types for BeeGFS values (`bflag` only when wrapping an external CLI). Hidden only with a stated reason.
- [ ] Messages: `"<Noun> created: <id>"` / `"Successfully <verb-ed> …"`; `WARNING: `; `Note: `.
- [ ] Canonical terminology in all user-facing strings.
