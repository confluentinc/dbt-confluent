from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp


class TestSimpleMaterializationsConfluent(BaseSimpleMaterializations):
    pass


class TestSingularTestsConfluent(BaseSingularTests):
    pass


class TestSingularTestsEphemeralConfluent(BaseSingularTestsEphemeral):
    pass


class TestEmptyConfluent(BaseEmpty):
    pass


class TestEphemeralConfluent(BaseEphemeral):
    pass


class TestIncrementalConfluent(BaseIncremental):
    pass


class TestGenericTestsConfluent(BaseGenericTests):
    pass


class TestSnapshotCheckColsConfluent(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampConfluent(BaseSnapshotTimestamp):
    pass


class TestBaseAdapterMethodConfluent(BaseAdapterMethod):
    pass
