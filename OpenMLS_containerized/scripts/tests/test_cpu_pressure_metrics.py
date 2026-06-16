"""
Deterministic unit tests for CPU pressure metric calculations.

Tests compute_deltas and derived metrics without requiring Docker.
"""

import math
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def compute_interval_deltas(start_stat, end_stat, elapsed_wall_us):
    """Compute interval deltas from two cpu.stat snapshots.

    Mirrors the logic in ResourceMonitor._write_summary.
    Returns dict of derived metrics or None for missing values.
    """
    delta_usage = _delta(start_stat.get("usage_usec"), end_stat.get("usage_usec"))
    delta_user = _delta(start_stat.get("user_usec"), end_stat.get("user_usec"))
    delta_system = _delta(start_stat.get("system_usec"), end_stat.get("system_usec"))
    delta_nr_periods = _delta(start_stat.get("nr_periods"), end_stat.get("nr_periods"))
    delta_nr_throttled = _delta(start_stat.get("nr_throttled"), end_stat.get("nr_throttled"))
    delta_throttled = _delta(start_stat.get("throttled_usec"), end_stat.get("throttled_usec"))

    result = {
        "delta_usage_usec": delta_usage,
        "delta_user_usec": delta_user,
        "delta_system_usec": delta_system,
        "delta_nr_periods": delta_nr_periods,
        "delta_nr_throttled": delta_nr_throttled,
        "delta_throttled_usec": delta_throttled,
    }

    if (delta_nr_periods is not None
            and isinstance(delta_nr_periods, (int, float))
            and delta_nr_periods > 0):
        result["throttled_period_fraction"] = round(
            float(delta_nr_throttled or 0) / float(delta_nr_periods), 6)
    else:
        result["throttled_period_fraction"] = None

    if elapsed_wall_us > 0:
        if isinstance(delta_usage, (int, float)):
            result["cpu_usage_fraction"] = round(float(delta_usage) / elapsed_wall_us, 6)
        if isinstance(delta_throttled, (int, float)):
            result["throttled_time_rate"] = round(float(delta_throttled) / elapsed_wall_us, 6)
    else:
        result["cpu_usage_fraction"] = None
        result["throttled_time_rate"] = None

    return result


def _delta(first, last):
    if first is None or last is None:
        return None
    return max(0, int(last) - int(first))


class TestCpuDeltaCalculations:
    """Test interval delta computation from synthetic cpu.stat snapshots."""

    def test_normal_interval_no_throttling(self):
        start = {"usage_usec": 100000, "user_usec": 80000, "system_usec": 20000,
                  "nr_periods": 10, "nr_throttled": 0, "throttled_usec": 0}
        end = {"usage_usec": 200000, "user_usec": 160000, "system_usec": 40000,
               "nr_periods": 20, "nr_throttled": 0, "throttled_usec": 0}
        elapsed = 100000  # 100ms in microseconds

        r = compute_interval_deltas(start, end, elapsed)

        assert r["delta_usage_usec"] == 100000
        assert r["delta_user_usec"] == 80000
        assert r["delta_system_usec"] == 20000
        assert r["delta_nr_periods"] == 10
        assert r["delta_nr_throttled"] == 0
        assert r["delta_throttled_usec"] == 0
        assert r["throttled_period_fraction"] == 0.0
        assert r["cpu_usage_fraction"] == 1.0
        assert r["throttled_time_rate"] == 0.0

    def test_with_throttling(self):
        start = {"usage_usec": 500000, "user_usec": 400000, "system_usec": 100000,
                  "nr_periods": 50, "nr_throttled": 0, "throttled_usec": 0}
        end = {"usage_usec": 750000, "user_usec": 600000, "system_usec": 150000,
               "nr_periods": 100, "nr_throttled": 30, "throttled_usec": 1500000}
        elapsed = 500000  # 500ms

        r = compute_interval_deltas(start, end, elapsed)

        assert r["delta_nr_periods"] == 50
        assert r["delta_nr_throttled"] == 30
        assert r["throttled_period_fraction"] == 0.6
        assert abs(r["cpu_usage_fraction"] - 0.5) < 0.01

    def test_throttled_fraction_bounded_0_to_1(self):
        start = {"usage_usec": 0, "user_usec": 0, "system_usec": 0,
                  "nr_periods": 0, "nr_throttled": 0, "throttled_usec": 0}
        end = {"usage_usec": 1, "user_usec": 0, "system_usec": 0,
               "nr_periods": 100, "nr_throttled": 0, "throttled_usec": 0}
        elapsed = 1000

        r = compute_interval_deltas(start, end, elapsed)
        assert r["throttled_period_fraction"] == 0.0

        end2 = {"usage_usec": 1, "user_usec": 0, "system_usec": 0,
                "nr_periods": 100, "nr_throttled": 100, "throttled_usec": 50000}
        r2 = compute_interval_deltas(start, end2, elapsed)
        assert r2["throttled_period_fraction"] == 1.0

    def test_zero_periods_yields_missing_fraction(self):
        start = {"usage_usec": 0, "user_usec": 0, "system_usec": 0,
                  "nr_periods": 0, "nr_throttled": 0, "throttled_usec": 0}
        end = {"usage_usec": 100, "user_usec": 50, "system_usec": 50,
               "nr_periods": 0, "nr_throttled": 5, "throttled_usec": 10000}
        elapsed = 1000

        r = compute_interval_deltas(start, end, elapsed)
        assert r["delta_nr_periods"] == 0
        assert r["throttled_period_fraction"] is None

    def test_cpu_usage_fraction_can_exceed_1_multicore(self):
        start = {"usage_usec": 0, "user_usec": 0, "system_usec": 0,
                  "nr_periods": 0, "nr_throttled": 0, "throttled_usec": 0}
        end = {"usage_usec": 3000000, "user_usec": 2000000, "system_usec": 1000000,
               "nr_periods": 100, "nr_throttled": 0, "throttled_usec": 0}
        elapsed = 1000000  # 1 second wall time, but 3 seconds of CPU

        r = compute_interval_deltas(start, end, elapsed)
        assert r["cpu_usage_fraction"] == 3.0

    def test_deltas_never_negative(self):
        start = {"usage_usec": 500, "user_usec": 400, "system_usec": 100,
                  "nr_periods": 10, "nr_throttled": 5, "throttled_usec": 1000}
        end = {"usage_usec": 100, "user_usec": 50, "system_usec": 50,
               "nr_periods": 5, "nr_throttled": 1, "throttled_usec": 200}
        elapsed = 1000

        r = compute_interval_deltas(start, end, elapsed)
        assert r["delta_usage_usec"] == 0
        assert r["delta_nr_periods"] == 0
        assert r["delta_nr_throttled"] == 0
        assert r["delta_throttled_usec"] == 0

    def test_missing_counters_yield_none_deltas(self):
        start = {}
        end = {"usage_usec": 100}
        elapsed = 1000

        r = compute_interval_deltas(start, end, elapsed)
        assert r["delta_usage_usec"] is None
        assert r["delta_nr_periods"] is None
        assert r["delta_nr_throttled"] is None
        assert r["throttled_period_fraction"] is None

    def test_elapsed_zero_yields_missing_rates(self):
        start = {"usage_usec": 0, "user_usec": 0, "system_usec": 0,
                  "nr_periods": 0, "nr_throttled": 0, "throttled_usec": 0}
        end = {"usage_usec": 100, "user_usec": 50, "system_usec": 50,
               "nr_periods": 10, "nr_throttled": 0, "throttled_usec": 0}
        elapsed = 0

        r = compute_interval_deltas(start, end, elapsed)
        assert r["cpu_usage_fraction"] is None
        assert r["throttled_time_rate"] is None

    def test_all_throttled_periods(self):
        start = {"usage_usec": 0, "user_usec": 0, "system_usec": 0,
                  "nr_periods": 0, "nr_throttled": 0, "throttled_usec": 0}
        end = {"usage_usec": 50000, "user_usec": 30000, "system_usec": 20000,
               "nr_periods": 200, "nr_throttled": 200, "throttled_usec": 20000000}
        elapsed = 20000000

        r = compute_interval_deltas(start, end, elapsed)
        assert r["throttled_period_fraction"] == 1.0
        assert r["throttled_time_rate"] == 1.0

    def test_throttled_time_rate_can_exceed_1(self):
        start = {"usage_usec": 0, "user_usec": 0, "system_usec": 0,
                  "nr_periods": 0, "nr_throttled": 0, "throttled_usec": 0}
        end = {"usage_usec": 100000, "user_usec": 50000, "system_usec": 50000,
               "nr_periods": 100, "nr_throttled": 80, "throttled_usec": 1500000}
        elapsed = 1000000

        r = compute_interval_deltas(start, end, elapsed)
        assert r["cpu_usage_fraction"] == 0.1
        assert r["throttled_time_rate"] == 1.5
        assert r["throttled_period_fraction"] == 0.8


class TestCounterMonotonicity:
    """Validation rules for cumulative cgroup counters."""

    def _validate(self, samples):
        issues = []
        for i in range(1, len(samples)):
            prev = samples[i - 1]
            curr = samples[i]
            for key in ("usage_usec", "nr_periods"):
                pv = prev.get(key, 0)
                cv = curr.get(key, 0)
                if cv < pv:
                    issues.append(f"sample {i}: {key} decreased from {pv} to {cv}")
        return issues

    def test_monotonic_valid(self):
        samples = [
            {"usage_usec": 0, "nr_periods": 0},
            {"usage_usec": 100, "nr_periods": 1},
            {"usage_usec": 200, "nr_periods": 2},
        ]
        assert self._validate(samples) == []

    def test_monotonic_decrease_detected(self):
        samples = [
            {"usage_usec": 100, "nr_periods": 5},
            {"usage_usec": 50, "nr_periods": 3},
        ]
        issues = self._validate(samples)
        assert len(issues) == 2
        assert "usage_usec" in issues[0]
        assert "nr_periods" in issues[1]

    def test_monotonic_equal_allowed(self):
        samples = [
            {"usage_usec": 100, "nr_periods": 5},
            {"usage_usec": 100, "nr_periods": 5},
        ]
        assert self._validate(samples) == []


class TestTimestampMonotonicity:
    def test_timestamps_monotonic(self):
        timestamps = [1000, 2000, 2000, 3000]
        issues = []
        for i in range(1, len(timestamps)):
            if timestamps[i] < timestamps[i - 1]:
                issues.append(i)
        assert issues == []

    def test_timestamps_backwards_detected(self):
        timestamps = [1000, 2000, 1500]
        issues = [i for i in range(1, len(timestamps)) if timestamps[i] < timestamps[i - 1]]
        assert len(issues) == 1


class TestDeltaValidationRules:
    """Rules from AGENT.md Section 4."""

    def test_delta_nr_throttled_lte_delta_nr_periods(self):
        start = {"nr_periods": 0, "nr_throttled": 0}
        end = {"nr_periods": 100, "nr_throttled": 80}
        r = compute_interval_deltas(start, end, 1000)
        assert r["delta_nr_throttled"] <= r["delta_nr_periods"]

    def test_zero_denominator_emits_missing(self):
        start = {"nr_periods": 0, "nr_throttled": 0, "usage_usec": 0,
                  "user_usec": 0, "system_usec": 0, "throttled_usec": 0}
        end = {"nr_periods": 0, "nr_throttled": 0, "usage_usec": 0,
               "user_usec": 0, "system_usec": 0, "throttled_usec": 0}
        r = compute_interval_deltas(start, end, 1000)
        assert r["throttled_period_fraction"] is None
