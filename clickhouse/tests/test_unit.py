# (C) Datadog, Inc. 2019
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
from collections import OrderedDict

import pytest

from datadog_checks.clickhouse.queries import SystemEvents, SystemMetrics

from .utils import ensure_csv_safe, parse_described_metrics

pytestmark = pytest.mark.unit

# The order is used to derive the display name for the parametrized tests
SYSTEM_TABLES = OrderedDict()


@pytest.mark.parametrize(
    'query_class, metric_source_url',
    [
        (
            SYSTEM_TABLES.setdefault(SystemMetrics.__name__, SystemMetrics),
            'https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/dbms/src/Common/CurrentMetrics.cpp',
        ),
        (
            SYSTEM_TABLES.setdefault(SystemEvents.__name__, SystemEvents),
            'https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/dbms/src/Common/ProfileEvents.cpp',
        ),
    ],
    ids=list(SYSTEM_TABLES),
)
def test_current_support(query_class, metric_source_url):
    # While we're here, also check key order
    assert list(query_class.columns) == sorted(query_class.columns)

    described_metrics = parse_described_metrics(metric_source_url)

    difference = set(described_metrics).difference(query_class.columns).difference(query_class.ignored_columns)

    if difference:
        num_metrics = len(difference)
        raise AssertionError(
            '{} has {} newly documented metric{}!\n{}'.format(
                query_class.__name__,
                num_metrics,
                's' if num_metrics > 1 else '',
                '\n'.join(
                    '---> {} | {}'.format(metric, ensure_csv_safe(described_metrics[metric]))
                    for metric in sorted(difference)
                ),
            )
        )
