# Decision Checklists — Confluent Python Projects

_Read during final review before committing Python code, or when you're unsure if you've followed all conventions._

---

## Before Writing `try/except`

- [ ] Is this at an error boundary? (CLI/API level)
- [ ] Can I check the condition proactively with a cheap, precise test (LBYL)?
- [ ] If not, is a small `try/except` around the authoritative operation clearer?
- [ ] Am I adding meaningful context, or just hiding the error?
- [ ] Is a third-party API forcing me to use exceptions?
- [ ] Am I catching specific exceptions (not bare `except:` or `except Exception:`)?
- [ ] If catching at error boundary: am I logging/warning? (Never silently swallow)
- [ ] Am I chaining with `from e` or `from None`? (ruff B904)

**Default: let exceptions bubble up**

---

## Before Path Operations

- [ ] Am I using `pathlib.Path`, not `os.path`?
- [ ] Did I specify `encoding="utf-8"` on `.read_text()` / `.write_text()`?
- [ ] Am I calling `.exists()` only because filesystem presence matters here?
- [ ] If missing paths should fail during `.resolve()`, did I pass `strict=True`?
- [ ] Am I treating `.is_relative_to()` as a bool check (not wrapping for `ValueError`)?

---

## Before Defining Interfaces (ABC or Protocol)

- [ ] Do I own all implementations? → Prefer **ABC**
- [ ] Am I wrapping a third-party library? → Prefer **Protocol**
- [ ] Do I need runtime `isinstance()` validation? → Use **ABC**
- [ ] Is this a minimal 1–2 method interface? → Protocol may be simpler
- [ ] Do I need shared method implementations? → Use **ABC**

**Default for internal Confluent code: ABC. For external library facades: Protocol.**

---

## Before Preserving Backwards Compatibility

- [ ] Did the user explicitly request it?
- [ ] Is this a public API with external consumers?
- [ ] Have I documented why it's needed in `CONTRIBUTING.md`?
- [ ] Is migration cost prohibitively high?

**Default: break the API and migrate callsites immediately**

---

## Before Using Inline Imports

- [ ] Is this to break a circular dependency?
- [ ] Is this for `TYPE_CHECKING`?
- [ ] Is this for a conditional optional feature?
- [ ] If for startup time: have I measured the import cost?
- [ ] If for startup time: is the cost significant (>100ms)?
- [ ] Have I documented why the inline import is needed in a comment?

**Default: module-level imports**

---

## Before Re-Exporting or Importing Symbols

- [ ] Does a canonical location already exist for this symbol?
- [ ] Am I creating a second import path for the same symbol?
- [ ] Have I avoided `__all__` exports?
- [ ] If this is a plugin entry point: am I using `from x import y as y` syntax?

**Default: import from canonical location, never re-export**

---

## Before Declaring a Local Variable

- [ ] Is this variable used more than once?
- [ ] Is this variable used close to where it's declared?
- [ ] Would inlining the computation hurt readability?
- [ ] Am I extracting object fields into locals that are only used once?

**Default: inline single-use computations; access object attributes directly**

---

## Before Adding a Function with 5+ Parameters

- [ ] Have I added `*` after the first (or `ctx`) parameter to make rest keyword-only?
- [ ] Is only `self`/`ctx` positional?
- [ ] Could I group related params into a Pydantic model?

**Default: keyword-only after the first parameter**

---

## Before Writing Module-Level Code

- [ ] Does this involve any computation (even `Path()` construction)?
- [ ] Does this involve I/O (file, network, environment)?
- [ ] Could this fail or raise exceptions?
- [ ] Would tests need to mock this value?

If any answer is "yes", wrap in a `@cache`-decorated function instead.

---

## Before Releasing (Project Standards Check)

- [ ] `pyproject.toml` present, no `setup.py` or `requirements.txt`
- [ ] `uv.lock` committed and up to date
- [ ] `ruff check .` passes with no errors
- [ ] `ruff format --check .` passes
- [ ] `mypy src` passes (strict mode)
- [ ] All tests pass: `pytest tests/unit` and `pytest tests/functional`
- [ ] Coverage floor met (≥80% on `src/`)
- [ ] `pip-audit` clean
- [ ] Changelog fragment added (towncrier) or commit message follows Conventional Commits
- [ ] GitHub Actions pinned to commit SHAs (not floating tags)
