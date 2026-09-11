"""Tableflow config translation/validation, and hand-rolled diffing of a model's declared
`tableflow` config against Confluent Cloud's live Tableflow state (#101).

Diffing is deliberately NOT generic JSON-Merge-Patch-over-wire-dicts: Tableflow's PATCH endpoint
doesn't behave like a plain RFC 7386 target. In particular, `config.error_handling` is a
discriminated union (`mode` plus mode-specific fields, e.g. `target` for `'LOG'`) and the server
needs `mode` present in the patch even when only a sibling field changed -- a naive per-field
diff can produce a patch that omits it. `compute_tableflow_patch` avoids this by comparing each
patchable field as its own typed value (`TableFormat`/`int`/`TableflowErrorHandling`, all
already parsed for us by `confluent_sql`) rather than diffing wire dicts field-by-field:
`error_handling` in particular is compared and replaced as one dataclass, always re-emitted via
its own `to_spec()` (which always includes `mode`), so the discriminator is never at risk of
being dropped.
"""

import json
from dataclasses import dataclass
from typing import NoReturn

from confluent_sql import (
    AzureAdlsStorage,
    ByobAwsStorage,
    InterfaceError,
    ManagedStorage,
    TableflowErrorHandling,
    TableflowErrorHandlingLog,
    TableflowErrorHandlingSkip,
    TableflowErrorHandlingSuspend,
    TableflowTopic,
    TableflowTopicConfig,
    TableFormat,
)
from confluent_sql import Error as ConfluentSqlError
from confluent_sql.exceptions import (
    ProgrammingError,
    TableflowTopicAlreadyExistsError,
    TableflowTopicNotFoundError,
)
from confluent_sql.tableflow import Fields, normalize_table_formats
from dbt_common.exceptions import CompilationError, DbtDatabaseError

from dbt.adapters.base import BaseRelation
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("Confluent")

# Each `tableflow.storage.kind`/`error_handling.mode` value, verbatim as the
# API/driver name it (`ManagedStorage.kind`, `TableflowErrorHandlingSuspend.mode`,
# etc.), mapped to confluent_sql's own class -- a thin passthrough, not a
# dbt-invented shorthand, so there's no separate naming for users to learn.
# Each shape's actual required/allowed fields are NOT hand-copied here;
# they're enforced by the dataclass constructor itself (see
# translate_tableflow_storage / translate_tableflow_error_handling), so a
# field the driver adds or renames needs no change on this side to be
# correctly accepted or rejected.
_TABLEFLOW_STORAGE_CLASSES: dict[str, type] = {
    ManagedStorage.kind: ManagedStorage,
    ByobAwsStorage.kind: ByobAwsStorage,
    AzureAdlsStorage.kind: AzureAdlsStorage,
}
_TABLEFLOW_ERROR_HANDLING_CLASSES: dict[str, type] = {
    TableflowErrorHandlingSuspend.mode: TableflowErrorHandlingSuspend,
    TableflowErrorHandlingSkip.mode: TableflowErrorHandlingSkip,
    TableflowErrorHandlingLog.mode: TableflowErrorHandlingLog,
}

# The only keys `tableflow` itself accepts. Checked explicitly (not just left to
# fall out of the translation below) because every one of `translate_tableflow_*`
# reads its fields by name via .get()/[...] -- an unknown/misspelled key would
# otherwise be silently ignored rather than rejected. Mirrors the API/driver's
# own `spec` shape verbatim: `table_formats`/`storage` sit alongside a nested
# `config`, not flattened -- see `_TABLEFLOW_TOPIC_CONFIG_KEYS`.
_TABLEFLOW_CONFIG_KEYS = frozenset({Fields.TABLE_FORMATS, Fields.STORAGE, Fields.CONFIG})

# The only keys `tableflow.config` itself accepts -- same passthrough
# philosophy, one level down, mirroring the API/driver's own `spec.config`.
_TABLEFLOW_TOPIC_CONFIG_KEYS = frozenset(
    {Fields.RETENTION_MS, Fields.DATA_RETENTION_MS, Fields.ERROR_HANDLING}
)


@dataclass(frozen=True)
class TableflowDesiredState:
    """The parsed, validated form of a model's `tableflow` config -- everything dbt actually
    declares and controls, and nothing else. Deliberately not shaped like the driver's
    `TableflowTopicSpec`: no `display_name`/`environment_id`/`kafka_cluster_id`/`suspended`,
    since `tableflow_config` never supplies any of those and dbt isn't allowed to touch them
    -- so there's nothing here that would need a fabricated value to build one.
    """

    table_formats: list[TableFormat]
    storage: ManagedStorage | ByobAwsStorage | AzureAdlsStorage
    config: TableflowTopicConfig | None

    @classmethod
    def from_spec(cls, tableflow_config: object) -> "TableflowDesiredState":
        """Validate `tableflow_config` (raising `CompilationError` on anything malformed --
        this is the only validation `tableflow` gets) and parse it into typed driver objects,
        once, here -- `create_tableflow_topic`/`compute_tableflow_patch` both take the result
        directly rather than each re-parsing the raw dict themselves.

        Typed `object`, not `dict`: this is the boundary where a user's own (untyped,
        YAML-sourced) `tableflow` config first gets checked, so the `isinstance` check right
        below is real, reachable validation -- not dead code a type checker could optimize
        away by trusting a `dict` annotation the actual caller doesn't honor.

        Delegates as much as possible to confluent_sql's own types (`translate_tableflow_*`
        below): a format/storage-kind/error-handling-mode the driver adds tomorrow is
        accepted here with no adapter change.
        """
        if not isinstance(tableflow_config, dict):
            raise CompilationError("'tableflow' config must be a mapping.")
        unknown = set(tableflow_config) - _TABLEFLOW_CONFIG_KEYS
        if unknown:
            raise CompilationError(
                f"'tableflow' has unknown key(s): {', '.join(sorted(unknown))}. "
                f"Allowed keys: {', '.join(sorted(_TABLEFLOW_CONFIG_KEYS))}."
            )
        return cls(
            table_formats=translate_table_formats(tableflow_config.get(Fields.TABLE_FORMATS)),
            storage=translate_tableflow_storage(tableflow_config.get(Fields.STORAGE)),
            config=translate_tableflow_topic_config(tableflow_config.get(Fields.CONFIG)),
        )

    def to_spec(self) -> dict:
        """Render back to a wire-shaped, JSON-serializable dict -- same convention as the
        driver's own `to_spec()` methods (`TableflowTopicConfig.to_spec()`,
        `TableflowStorage.to_spec()`, ...), which this delegates to for `storage`/`config`.
        Only for logging/debugging -- nothing here reconstructs an actual request payload.
        """
        return {
            Fields.TABLE_FORMATS: list(self.table_formats),
            Fields.STORAGE: self.storage.to_spec(),
            Fields.CONFIG: self.config.to_spec() if self.config is not None else {},
        }


@dataclass(frozen=True)
class TableflowPatch:
    """The subset of `TableflowDesiredState` that's actually changing, in the exact shape
    `Connection.patch_tableflow` takes -- `None` on either field means "leave unchanged,"
    same convention the driver itself uses. `config`, when present, is already sparse: only
    the sub-fields that changed are set, the rest are `None` (also "leave unchanged").
    """

    table_formats: list[TableFormat] | None
    config: TableflowTopicConfig | None

    def to_spec(self) -> dict:
        """Render to a wire-shaped, JSON-serializable dict, for logging/debugging only --
        same convention as `TableflowDesiredState.to_spec()`."""
        spec: dict = {}
        if self.table_formats is not None:
            spec[Fields.TABLE_FORMATS] = list(self.table_formats)
        if self.config is not None:
            spec[Fields.CONFIG] = self.config.to_spec()
        return spec


def reconcile_tableflow_config(
    handle, relation: BaseRelation, tableflow_config: dict | None
) -> None:
    """Ensure `relation`'s backing Kafka topic reflects `config(tableflow={...})`.

    Shape::

        tableflow={
            'table_formats': ['ICEBERG', 'DELTA'],  # required
            'storage': {'kind': 'Managed'}
                     | {'kind': 'ByobAws', 'bucket_name': ..., 'provider_integration_id': ...}
                     | {'kind': 'AzureDataLakeStorageGen2', 'storage_account_name': ...,
                        'container_name': ..., 'provider_integration_id': ...},  # required
            'config': {                      # optional
                'retention_ms': 604800000,       # optional
                'data_retention_ms': 604800000,  # optional
                'error_handling': {'mode': 'SUSPEND' | 'SKIP'}
                                | {'mode': 'LOG', 'target': '...'},  # optional
            },
        }

    No-op if `tableflow_config` is empty/None -- config governs whether
    we touch Tableflow at all; live state is only ever consulted once
    we already know the model wants to manage it.

    Runs the same way on every invocation:

    - Not enabled -> enable it with the current config.
    - Already enabled, `storage` changed -> `storage`/`display_name` are immutable via
      PATCH, but not actually unchangeable: `recreate_tableflow_topic` disables and
      re-enables Tableflow to apply it. That only ever touches the Tableflow sink, never
      the underlying Kafka topic or its data -- unlike `--full-refresh`, which drops and
      recreates the topic itself -- and re-enabling backfills the full topic history from
      the earliest offset, so nothing here leaves a coverage gap (#101).
    - Already enabled, only `table_formats`/`config` changed -> diff the live config
      against the current `tableflow` config (`compute_tableflow_patch`) and PATCH only
      if something actually changed, so an unchanged config is a true no-op rather than
      cycling the backing materialization job every run.

    Calls the driver's `Connection` directly (no SQL statement, no cursor),
    bypassing `exception_handler`'s usual confluent_sql -> DbtDatabaseError
    wrapping -- so any error other than the expected "already enabled"/
    "already exists" outcomes is wrapped here instead.
    """
    if not tableflow_config:
        return

    # Raises CompilationError on anything malformed -- this is the only validation
    # `tableflow` gets, so it must run before touching the driver at all (including
    # the `probe_tableflow_state` GET below), not get masked by a connection/auth
    # error, and not silently skipped when the relation already exists.
    desired = TableflowDesiredState.from_spec(tableflow_config)
    logger.debug("Desired Tableflow state: " + json.dumps(desired.to_spec(), indent=2))

    existing = probe_tableflow_state(handle, relation)
    if existing is None:
        create_tableflow_topic(handle, relation, desired)
        return
    logger.debug("Current Tableflow state: " + json.dumps(existing.spec.raw, indent=2))

    if desired.storage != existing.spec.storage:
        recreate_tableflow_topic(handle, relation, desired)
        return

    patch = compute_tableflow_patch(existing, desired)
    if patch is None:
        return

    logger.debug("Generated Tableflow patch: " + json.dumps(patch.to_spec(), indent=2))
    patch_tableflow_topic(handle, relation, patch)


def probe_tableflow_state(handle, relation: BaseRelation) -> TableflowTopic | None:
    """GET `relation`'s live Tableflow state, or None if not enabled.

    Shared by `reconcile_tableflow_config` and `disable_tableflow_if_enabled`,
    which otherwise duplicate this exact GET-and-translate-errors step --
    each still decides for itself what a hit/miss means (fall through to
    enable vs. warn-and-return; no-op vs. proceed to disable).
    """
    try:
        return handle.get_tableflow(relation.identifier)
    except TableflowTopicNotFoundError:
        return None
    except ProgrammingError as e:
        reraise_tableflow_auth_error(e)
    except ConfluentSqlError as e:
        raise DbtDatabaseError(f"Error checking Tableflow state for {relation}: {e}") from e


def disable_tableflow_topic(handle, relation: BaseRelation) -> None:
    """DELETE Tableflow on `relation`, tolerating a narrow "already gone" race.

    Shared by `disable_tableflow_if_enabled` (before a DROP TABLE) and
    `recreate_tableflow_topic` (to apply a `storage`/`display_name` change) -- each still
    decides for itself whether/when to call this and what to log around it.
    """
    try:
        handle.disable_tableflow(relation.identifier)
    except TableflowTopicNotFoundError:
        return  # Narrow race: already gone.
    except ProgrammingError as e:
        reraise_tableflow_auth_error(e)
    except ConfluentSqlError as e:
        raise DbtDatabaseError(f"Error disabling Tableflow for {relation}: {e}") from e


def recreate_tableflow_topic(
    handle, relation: BaseRelation, desired: TableflowDesiredState
) -> TableflowTopic | None:
    """Disable and re-enable Tableflow to apply a `storage`/`display_name` change that has no
    in-place PATCH path (#101).

    Disable/enable only ever touch the Tableflow sink, never the underlying Kafka topic or its
    data -- unlike `--full-refresh`, which drops and recreates the topic itself -- and
    re-enabling backfills the full topic history from the earliest offset, so nothing here
    leaves a coverage gap. `table_formats`/`config` are (re-)applied at the same time, from
    `desired`, same as any other create.
    """
    logger.info(
        f"Tableflow storage config changed for {relation}; disabling and re-enabling to apply "
        f"it -- this rebuilds from the full topic history and can take a while."
    )
    disable_tableflow_topic(handle, relation)
    return create_tableflow_topic(handle, relation, desired)


def compute_tableflow_patch(
    existing: TableflowTopic, desired: TableflowDesiredState
) -> TableflowPatch | None:
    """Diff `existing`'s live state against `desired`, returning the `TableflowPatch` needed to
    reconcile them, or None if nothing's changed.

    Compares each patchable field as its own typed value -- `existing.spec` is already parsed
    into `TableFormat`/`TableflowTopicConfig`/`TableflowErrorHandling` by `confluent_sql`, so
    there's no wire-shape juggling (int-vs-string retention, single-format-vs-list, ...) here at
    all. `error_handling` is compared and replaced as one whole dataclass (`!=`), never diffed
    field-by-field -- `TableflowPatch.config.error_handling`, when set, is `desired`'s own
    `TableflowErrorHandling` object, later re-emitted via its own `to_spec()` (which always
    includes `mode`) by the driver, so the discriminator can never be dropped from a patch that
    changes one of its siblings (see module docstring).

    `table_formats`/`config` are the only patchable fields; `storage`/`display_name` are
    immutable via this API and are never compared.

    `retention_ms`/`data_retention_ms`/`error_handling` are never cleared even when dbt's
    config omits them -- they're optional on the request, but not on the resource itself, and
    the server rejects an explicit null for any of them. `tableflow`'s otherwise-declarative
    semantics (omitted means "shouldn't exist") don't apply to these three: once set, by dbt
    or otherwise, they can only be changed to a new value, never unset, via this API.
    """
    desired_config = desired.config if desired.config is not None else TableflowTopicConfig()
    existing_config = (
        existing.spec.config if existing.spec.config is not None else TableflowTopicConfig()
    )

    # retention_ms/data_retention_ms/error_handling are optional on the request but not on
    # the resource itself -- the server rejects an explicit null for any of them. So when
    # dbt's config omits one, it's left alone entirely (never included in the patch), rather
    # than treated as "should be cleared" the way the rest of `tableflow` is: once set (by
    # dbt or otherwise), these three can only be changed to a new value, never unset, via
    # this API.
    config = TableflowTopicConfig(
        retention_ms=(
            desired_config.retention_ms
            if desired_config.retention_ms is not None
            and desired_config.retention_ms != existing_config.retention_ms
            else None
        ),
        data_retention_ms=(
            desired_config.data_retention_ms
            if desired_config.data_retention_ms is not None
            and desired_config.data_retention_ms != existing_config.data_retention_ms
            else None
        ),
        error_handling=(
            desired_config.error_handling
            if desired_config.error_handling is not None
            and desired_config.error_handling != existing_config.error_handling
            else None
        ),
    )

    patch = TableflowPatch(
        table_formats=(
            desired.table_formats if desired.table_formats != existing.spec.table_formats else None
        ),
        config=config if config != TableflowTopicConfig() else None,
    )
    return patch if patch != TableflowPatch(table_formats=None, config=None) else None


def create_tableflow_topic(
    handle, relation: BaseRelation, desired: TableflowDesiredState
) -> TableflowTopic | None:
    """Enable Tableflow on `relation` with `desired`."""
    # Blocks (by default) until the topic reaches RUNNING, up to 300s -- worth
    # logging at info, not debug, so the wait is visible without --debug.
    logger.info(f"Enabling Tableflow for {relation} ({desired!r}) -- this can take a few minutes.")
    try:
        return handle.enable_tableflow(
            relation.identifier,
            table_formats=desired.table_formats,
            storage=desired.storage,
            config=desired.config,
        )
    except TableflowTopicAlreadyExistsError:
        # Narrow race: something else enabled it between our GET above and
        # this call. The desired end state (enabled) already holds -- same
        # "swallowed, not warning-worthy" treatment as the symmetric race in
        # `patch_tableflow_topic` (disabled concurrently).
        logger.debug(f"Tableflow was enabled concurrently for {relation}; leaving as-is.")
        return probe_tableflow_state(handle, relation)
    except ProgrammingError as e:
        reraise_tableflow_auth_error(e)
    except ConfluentSqlError as e:
        raise DbtDatabaseError(f"Error enabling Tableflow for {relation}: {e}") from e


def patch_tableflow_topic(
    handle, relation: BaseRelation, patch: TableflowPatch
) -> TableflowTopic | None:
    """PATCH Tableflow on `relation` with the computed diff."""
    # Blocks (by default) until the topic reaches RUNNING, up to 300s -- worth
    # logging at info, not debug, so the wait is visible without --debug.
    logger.info(
        f"Updating Tableflow for {relation} to match changed `tableflow` "
        f"config -- this can take a few minutes."
    )
    try:
        return handle.update_tableflow(
            relation.identifier, table_formats=patch.table_formats, config=patch.config
        )
    except TableflowTopicNotFoundError:
        # Narrow race: disabled concurrently between our GET above and this
        # call. Leave it disabled -- the next run's GET will see that and
        # enable fresh with the full desired config, same as any other
        # not-yet-enabled model.
        logger.debug(f"Tableflow was disabled concurrently for {relation}; leaving as-is.")
        return None
    except ProgrammingError as e:
        reraise_tableflow_auth_error(e)
    except ConfluentSqlError as e:
        raise DbtDatabaseError(f"Error updating Tableflow for {relation}: {e}") from e


def translate_table_formats(formats: object) -> list[str]:
    """Validate and normalize `tableflow.table_formats` into the driver's wire
    list. Exact case, matching `storage.kind`/`error_handling.mode` --
    every `tableflow` discriminator is a thin passthrough of the API's
    own values, none case-insensitive. Which values are actually valid
    formats is entirely confluent_sql's `normalize_table_formats`'s
    call, not ours -- a format the driver adds tomorrow is accepted
    here with no adapter change.
    """
    raw = [formats] if isinstance(formats, str) else formats
    if (
        not formats
        or not isinstance(raw, (list, tuple))
        or not all(isinstance(f, str) for f in raw)
    ):
        raise CompilationError(
            "'tableflow.table_formats' is required and must be 'ICEBERG'/'DELTA' or a list of them."
        )
    try:
        return normalize_table_formats(raw)
    except InterfaceError as e:
        raise CompilationError(f"'tableflow.table_formats' is invalid: {e}") from e


def translate_tableflow_storage(
    storage: object,
) -> ManagedStorage | ByobAwsStorage | AzureAdlsStorage:
    """Validate and translate `tableflow.storage` into its driver type.

    Only the kind-to-class dispatch is ours; each storage kind's
    required/allowed *fields* are enforced by attempting the real
    dataclass construction and catching the `TypeError` it raises on a
    missing or unexpected field, instead of a hand-copied required-keys
    list that could silently go stale against the driver. Field *values*
    get their own check (see `_require_string_fields`) since the
    constructor won't catch a wrong-typed one.
    """
    if not isinstance(storage, dict) or Fields.KIND not in storage:
        raise CompilationError(
            "'tableflow.storage' is required and must be a mapping with a 'kind' "
            f"key ({', '.join(sorted(_TABLEFLOW_STORAGE_CLASSES))})."
        )
    storage_kind = storage[Fields.KIND]
    # isinstance guard first -- dict.get() raises TypeError, not a clean
    # CompilationError, on an unhashable kind (e.g. a list by mistake).
    storage_cls = (
        _TABLEFLOW_STORAGE_CLASSES.get(storage_kind) if isinstance(storage_kind, str) else None
    )
    if storage_cls is None:
        raise CompilationError(
            f"'tableflow.storage.kind' must be one of "
            f"{sorted(_TABLEFLOW_STORAGE_CLASSES)}; got {storage_kind!r}."
        )
    fields = {k: v for k, v in storage.items() if k != Fields.KIND}
    _require_string_fields(fields, "tableflow.storage")
    try:
        return storage_cls(**fields)
    except TypeError as e:
        raise CompilationError(
            f"'tableflow.storage' of kind '{storage_kind}' is invalid: {e}"
        ) from e


def translate_tableflow_error_handling(eh: object) -> TableflowErrorHandling:
    """Validate and translate `tableflow.config.error_handling` into its
    driver type. Same philosophy as `translate_tableflow_storage`: only
    the mode-name-to-class dispatch is ours, and each mode's allowed
    fields (e.g. `target` for `'LOG'` only) are enforced by the
    constructor itself, not a hand-copied per-mode key list -- field
    values are checked separately (see `_require_string_fields`).
    """
    if not isinstance(eh, dict) or Fields.MODE not in eh:
        raise CompilationError(
            "'tableflow.config.error_handling' must be a mapping with a 'mode' key."
        )
    mode = eh[Fields.MODE]
    # isinstance guard first -- dict.get() raises TypeError, not a clean
    # CompilationError, on an unhashable mode (e.g. a list by mistake).
    eh_cls = _TABLEFLOW_ERROR_HANDLING_CLASSES.get(mode) if isinstance(mode, str) else None
    if eh_cls is None:
        raise CompilationError(
            f"'tableflow.config.error_handling.mode' must be one of "
            f"{sorted(_TABLEFLOW_ERROR_HANDLING_CLASSES)}; got {mode!r}."
        )
    fields = {k: v for k, v in eh.items() if k != Fields.MODE}
    _require_string_fields(fields, "tableflow.config.error_handling")
    try:
        return eh_cls(**fields)
    except TypeError as e:
        raise CompilationError(
            f"'tableflow.config.error_handling' of mode '{mode}' is invalid: {e}"
        ) from e


def translate_tableflow_topic_config(config: object) -> TableflowTopicConfig | None:
    """Translate `tableflow.config` (`retention_ms`/`data_retention_ms`/
    `error_handling`) into a `TableflowTopicConfig`, or None if unset or
    empty (an empty config sends nothing extra in the request, matching
    the driver's own default).

    `tableflow.config` mirrors the API/driver's own `spec.config` nesting
    verbatim -- the same passthrough philosophy already applied to
    `storage.kind`/`error_handling.mode`'s field *names*, extended to
    shape as well, so a config copied straight from the API spec, the
    `confluent` CLI's own payload, or `confluent_sql` works unchanged
    instead of needing to be flattened into a dbt-invented shape.

    `retention_ms`/`data_retention_ms` get a sign/type check the driver's
    `str | int | None` field type doesn't itself enforce -- but this is
    timeless dimensional sanity (a duration can't be negative or a list),
    not a business rule tied to Confluent's current feature set, so
    there's no server-sync risk in keeping it eager here.
    """
    if config is None:
        return None
    if not isinstance(config, dict):
        raise CompilationError("'tableflow.config' must be a mapping.")
    unknown = set(config) - _TABLEFLOW_TOPIC_CONFIG_KEYS
    if unknown:
        raise CompilationError(
            f"'tableflow.config' has unknown key(s): {', '.join(sorted(unknown))}. "
            f"Allowed keys: {', '.join(sorted(_TABLEFLOW_TOPIC_CONFIG_KEYS))}."
        )

    for key in (Fields.RETENTION_MS, Fields.DATA_RETENTION_MS):
        value = config.get(key)
        if value is None:
            continue
        if isinstance(value, bool) or (
            not (isinstance(value, int) and value >= 0)
            and not (isinstance(value, str) and value.isdigit())
        ):
            raise CompilationError(
                f"'tableflow.config.{key}' must be a non-negative integer (or a numeric string)."
            )

    error_handling_conf = config.get(Fields.ERROR_HANDLING)
    error_handling = (
        translate_tableflow_error_handling(error_handling_conf)
        if error_handling_conf is not None
        else None
    )

    retention_ms = config.get(Fields.RETENTION_MS)
    data_retention_ms = config.get(Fields.DATA_RETENTION_MS)
    if retention_ms is None and data_retention_ms is None and error_handling is None:
        return None
    return TableflowTopicConfig(
        # Wire values are strings (or user-supplied ints); TableflowTopicConfig's own
        # fields are typed int, so normalize here -- the numeric check above already
        # guarantees these are digit-only strings or non-negative ints.
        retention_ms=int(retention_ms) if retention_ms is not None else None,
        data_retention_ms=int(data_retention_ms) if data_retention_ms is not None else None,
        error_handling=error_handling,
    )


def reraise_tableflow_auth_error(e: ProgrammingError) -> NoReturn:
    """Translate the driver's Kafka-cluster-id-resolution failure into
    guidance that names the actual profile field, or bubble it up
    unchanged if it's not that specific error.

    `ProgrammingError` covers more than this one case, so only the
    known "no global key" message (raised by `_resolve_kafka_cluster_id`
    when `database` can't be resolved to a Kafka cluster id) is
    rewritten. Anything else re-raises as-is rather than risk
    mislabeling an unrelated `ProgrammingError`.

    Only `global_api_key`/`global_api_secret` is offered as a fix: CMK
    cluster-id resolution requires the global key specifically, so a
    Tableflow-scoped key pair can't satisfy it -- this adapter doesn't
    expose `database_kafka_cluster_id` to skip the lookup instead (#105).
    """
    if "requires a global API key" not in str(e):
        raise e
    raise DbtDatabaseError(
        "Tableflow needs to resolve your Kafka cluster id, which requires a Global "
        "API key. Add `global_api_key`/`global_api_secret` to your profile -- see "
        "README.md#configuration."
    ) from e


def _require_string_fields(fields: dict, owner: str) -> None:
    """Raise if any of `fields`' values isn't a string.

    Every field the storage/error-handling dataclasses accept (bucket
    names, integration ids, dead-letter targets, ...) is typed `str` --
    but dataclasses don't enforce field types at construction, so a
    wrong-typed value would otherwise construct successfully and only
    fail (or silently misbehave) far downstream at the actual API call.
    A blanket "every field here is a string" is timeless sanity, not a
    business rule that could go stale, so it's checked explicitly
    instead of relying on the constructor. `owner` is the dotted config
    path used in the error message (e.g. "tableflow.storage").
    """
    for key, value in fields.items():
        if not isinstance(value, str):
            raise CompilationError(f"'{owner}.{key}' must be a string; got {value!r}.")
