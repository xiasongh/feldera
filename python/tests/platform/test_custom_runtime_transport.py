"""Cross-version test for transports provided only by a selected runtime."""

import os

import pytest
from feldera import PipelineBuilder
from feldera.enums import CompilationProfile
from tests import TEST_CLIENT


RUNTIME_VERSION = os.environ.get("FELDERA_CUSTOM_RUNTIME_VERSION")

pytestmark = pytest.mark.skipif(
    RUNTIME_VERSION is None,
    reason="requires a runtime prepared by scripts/test_custom_runtime_transport.sh",
)


def test_custom_runtime_transport(pipeline_name):
    sql = """
CREATE TABLE t (id BIGINT NOT NULL) WITH (
    'connectors' = '[{
        "name": "runtime-only-datagen",
        "transport": {
            "name": "runtime_only_datagen",
            "config": {"plan": [{"limit": 3}]}
        }
    }]'
);

CREATE MATERIALIZED VIEW row_count AS SELECT COUNT(*) AS c FROM t;
"""

    builder = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql,
        compilation_profile=CompilationProfile.DEV,
        runtime_version=RUNTIME_VERSION,
    )
    builder.use_platform_compiler = True
    pipeline = builder.create_or_replace()

    messages = pipeline.program_error()["sql_compilation"]["messages"]
    assert any(
        message["warning"]
        and message["error_type"] == "Unknown format"
        and '"runtime_only_datagen" is not known' in message["message"]
        for message in messages
    )

    pipeline.start()
    pipeline.wait_for_completion(timeout_s=300)

    assert list(pipeline.query("SELECT c FROM row_count")) == [{"c": 3}]
