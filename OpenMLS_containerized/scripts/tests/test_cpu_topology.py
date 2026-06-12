"""
Unit tests for cpu_topology.py
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from cpu_topology import (
    CpuInfo,
    CpuTopology,
    CpuStat,
    detect_cpu_topology,
    get_online_cpu_list,
    select_least_loaded_cpus,
    get_smt_siblings,
    topology_to_dict,
)


def make_topology(cpu_ids, online=None, core_map=None, smt_enabled=False):
    """Helper to create a mock CpuTopology."""
    topo = CpuTopology(smt_enabled=smt_enabled)
    online = online or cpu_ids
    core_map = core_map or {c: c for c in cpu_ids}

    for cpu_id in cpu_ids:
        core_id = core_map.get(cpu_id, cpu_id)
        topo.cpus[cpu_id] = CpuInfo(
            cpu_id=cpu_id,
            core_id=core_id,
            online=(cpu_id in online),
            is_smt_sibling=False,
        )
        if core_id not in topo.core_to_cpus:
            topo.core_to_cpus[core_id] = []
        topo.core_to_cpus[core_id].append(cpu_id)
        topo.total_cpu_count += 1
        if cpu_id in online:
            topo.online_cpu_count += 1

    if smt_enabled:
        for core_id, cpus in topo.core_to_cpus.items():
            if len(cpus) >= 2:
                for cpu in cpus:
                    topo.cpus[cpu].is_smt_sibling = (len(cpus) >= 2)

    return topo


class TestGetOnlineCpuList:
    def test_all_online(self):
        topo = make_topology([0, 1, 2, 3])
        assert get_online_cpu_list(topo) == [0, 1, 2, 3]

    def test_some_offline(self):
        topo = make_topology([0, 1, 2, 3], online=[0, 2])
        assert get_online_cpu_list(topo) == [0, 2]


class TestSelectLeastLoadedCpus:
    def test_select_one_least_loaded(self):
        topo = make_topology([0, 1, 2, 3])
        loads = {0: 0.9, 1: 0.1, 2: 0.5, 3: 0.8}
        selected = select_least_loaded_cpus(
            topo, required_count=1, load_samples=loads
        )
        assert selected == [1]

    def test_select_multiple(self):
        topo = make_topology([0, 1, 2, 3])
        loads = {0: 0.9, 1: 0.1, 2: 0.5, 3: 0.8}
        selected = select_least_loaded_cpus(
            topo, required_count=2, load_samples=loads
        )
        assert selected == [1, 2]

    def test_exclude_cpus(self):
        topo = make_topology([0, 1, 2, 3])
        loads = {0: 0.1, 1: 0.2, 2: 0.5, 3: 0.8}
        selected = select_least_loaded_cpus(
            topo, required_count=2, load_samples=loads,
            exclude_cpus=[0],
        )
        assert 0 not in selected
        assert selected == [1, 2]

    def test_insufficient_cpus_raises(self):
        topo = make_topology([0, 1])
        loads = {0: 0.1, 1: 0.2}
        with pytest.raises(ValueError, match="Insufficient online CPUs"):
            select_least_loaded_cpus(
                topo, required_count=3, load_samples=loads,
            )

    def test_stable_tiebreaking(self):
        topo = make_topology([3, 1, 0, 2])
        loads = {0: 0.5, 1: 0.5, 2: 0.5, 3: 0.5}
        selected = select_least_loaded_cpus(
            topo, required_count=2, load_samples=loads,
        )
        assert selected == [0, 1]

    def test_offline_excluded(self):
        topo = make_topology([0, 1, 2], online=[0, 2])
        loads = {0: 0.1, 1: 0.0, 2: 0.9}
        selected = select_least_loaded_cpus(
            topo, required_count=1, load_samples=loads,
        )
        assert 1 not in selected

    def test_missing_load_defaults_zero(self):
        topo = make_topology([0, 1, 2])
        loads = {0: 0.9}
        selected = select_least_loaded_cpus(
            topo, required_count=1, load_samples=loads,
        )
        assert selected == [1]


class TestSmtSiblings:
    def test_no_smt(self):
        topo = make_topology([0, 1, 2, 3])
        assert get_smt_siblings(topo, [0]) == []

    def test_smt_siblings_found(self):
        topo = make_topology([0, 1, 2, 3], core_map={0: 0, 1: 0, 2: 1, 3: 1}, smt_enabled=True)
        siblings = get_smt_siblings(topo, [0])
        assert 1 in siblings

    def test_smt_not_in_sibling_list(self):
        topo = make_topology([0, 1], core_map={0: 0, 1: 0}, smt_enabled=True)
        siblings = get_smt_siblings(topo, [0])
        assert 0 not in siblings


class TestTopologyToDict:
    def test_returns_valid_dict(self):
        topo = make_topology([0, 1])
        d = topology_to_dict(topo)
        assert d["online_cpu_count"] == 2
        assert d["total_cpu_count"] == 2
        assert "cpus" in d
        assert len(d["cpus"]) == 2


class TestCpuStat:
    def test_total(self):
        s = CpuStat(user=100, system=50, idle=200)
        assert s.total >= 350

    def test_busy(self):
        s = CpuStat(user=100, system=50, idle=200, iowait=10)
        assert s.busy == 150
