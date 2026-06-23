"""
Tests verifying that a selected resource profile propagates correctly
through affinity plan, worker assignments, and generated Compose config.

Targets Phase 1 of AGENT.md: Fix selected resource profile propagation.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from resource_profiles import (
    ResourceProfile,
    generate_ram_sweep_profiles,
    generate_cpu_matrix_profiles,
    generate_parallel_ram_sweep_profiles,
    generate_parallel_cpu_sweep_profiles,
    select_profile,
    select_all_profiles,
    get_selected_profile,
    select_profile_by_index,
    select_profile_by_id,
    profile_to_compose_dict,
)
from cpu_affinity_planner import (
    AffinityPlan,
    ProfiledAssignment,
    BackgroundAssignment,
)
from resource_experiment_runner import (
    build_worker_resource_assignments,
)


class TestSelectedProfileResolution:
    """Tests that select_profile and get_selected_profile work correctly."""

    def test_select_ram_index_0(self):
        profiles = generate_ram_sweep_profiles(["32m", "64m", "128m"], assigned_cpu_count=4)
        sp = select_profile(profiles, profile_index=0, profiled_singleton_count=1)
        assert sp.resource_profile_index == 0
        assert "32m" in sp.resource_profile_id or sp.memory_limit == "32m"

    def test_select_ram_index_3(self):
        profiles = generate_ram_sweep_profiles(
            ["32m", "64m", "128m", "256m", "512m", "1g"], assigned_cpu_count=10
        )
        sp = select_profile(profiles, profile_index=3, profiled_singleton_count=1)
        assert sp.resource_profile_index == 3
        assert "256m" in sp.resource_profile_id or sp.memory_limit == "256m"
        assert sp.selected_for_this_run is True

    def test_select_last_ram_index(self):
        profiles = generate_ram_sweep_profiles(
            ["32m", "64m", "128m", "256m", "512m", "1g"], assigned_cpu_count=10
        )
        sp = select_profile(profiles, profile_index=5, profiled_singleton_count=1)
        assert sp.resource_profile_index == 5
        assert "1g" in sp.resource_profile_id or sp.memory_limit == "1g"

    def test_select_cpu_index_0(self):
        profiles = generate_cpu_matrix_profiles(
            core_counts=[1, 2, 4], capacity_fractions=[0.25, 0.50, 0.75, 1.00]
        )
        sp = select_profile(profiles, profile_index=0, profiled_singleton_count=1)
        assert sp.resource_profile_index == 0
        assert sp.experiment_kind == "cpu_matrix_singleton"

    def test_select_cpu_index_5(self):
        profiles = generate_cpu_matrix_profiles(
            core_counts=[1, 2, 4], capacity_fractions=[0.25, 0.50, 0.75, 1.00]
        )
        sp = select_profile(profiles, profile_index=5, profiled_singleton_count=1)
        assert sp.resource_profile_index == 5
        assert sp.experiment_kind == "cpu_matrix_singleton"

    def test_select_last_cpu_index(self):
        profiles = generate_cpu_matrix_profiles(
            core_counts=[1, 2, 4], capacity_fractions=[0.25, 0.50, 0.75, 1.00]
        )
        last = len(profiles) - 1
        sp = select_profile(profiles, profile_index=last, profiled_singleton_count=1)
        assert sp.resource_profile_index == last

    def test_unknown_profile_id_raises(self):
        profiles = generate_ram_sweep_profiles(["32m", "64m"], assigned_cpu_count=4)
        try:
            select_profile(profiles, profile_id="ram_999m", profiled_singleton_count=1)
            assert False, "Expected ValueError"
        except ValueError as e:
            assert "Unknown" in str(e) or "ram_999m" in str(e)

    def test_multiple_selected_rejected(self):
        profiles = generate_ram_sweep_profiles(["32m", "64m"], assigned_cpu_count=4)
        # Setting multiple profiles as selected should not be silent
        for p in profiles:
            p.selected_for_this_run = True
        sps = [p for p in profiles if p.selected_for_this_run]
        # select_profile will set all to False and mark only one
        sp = select_profile(profiles, profile_index=0, profiled_singleton_count=1)
        assert sp.selected_for_this_run is True
        selected_count = sum(1 for p in profiles if p.selected_for_this_run)
        assert selected_count == 1

    def test_no_selection_required_for_singleton_count_1(self):
        profiles = generate_ram_sweep_profiles(["32m"], assigned_cpu_count=4)
        try:
            select_profile(profiles, profiled_singleton_count=1)
            assert False, "Expected ValueError when no index or ID given for sc=1"
        except ValueError:
            pass


class TestWorkerAssignmentPropagation:
    """Tests that nonzero selected profile reaches worker assignments."""

    def _make_minimal_plan(self, run_id, profiled_specs):
        """Create a minimal AffinityPlan for testing without hardware."""
        pa_list = []
        for spec in profiled_specs:
            pa_list.append(ProfiledAssignment(
                worker_id=spec["worker_id"],
                container_name=spec["container_name"],
                logical_client_id=spec["logical_client_id"],
                assigned_cpus=[0, 1, 2, 3],
                assigned_mask_hex="0xf",
                assigned_cpu_count=4,
                rayon_num_threads=spec.get("rayon_num_threads", 4),
                experiment_kind=spec.get("experiment_kind", ""),
                resource_profile_id=spec.get("resource_profile_id", ""),
            ))
        return AffinityPlan(
            run_id=run_id,
            created_at="2026-01-01T00:00:00",
            hostname="test-host",
            cpu_affinity_mode="profiled-nor-background",
            sample_seconds=0.1,
            selection_policy="least_loaded",
            online_cpus=[0, 1, 2, 3, 4, 5, 6, 7],
            online_cpu_mask_hex="0xff",
            cpu_topology={},
            sampled_cpu_load={},
            profiled_assignments=pa_list,
            profiled_mask_hex="0xf",
            reserved_mask_hex="0xf",
            background_cpus=[4, 5, 6, 7],
            background_mask_hex="0xf0",
            smt_sibling_policy="no_reservation",
            warnings=[],
            background_assignments=[
                BackgroundAssignment(
                    container_name="ds",
                    container_role="infrastructure",
                    assigned_cpus=[4, 5, 6, 7],
                    assigned_mask_hex="0xf0",
                ),
                BackgroundAssignment(
                    container_name="relay",
                    container_role="infrastructure",
                    assigned_cpus=[4, 5, 6, 7],
                    assigned_mask_hex="0xf0",
                ),
            ],
        )

    def test_ram_index_3_in_assignments(self):
        """Select RAM profile index 3; verify it appears in worker assignments.
        
        This test will FAIL before the fix because build_worker_resource_assignments
        uses profiles[i % len(profiles)] which always selects index 0 for one singleton.
        """
        profiles = generate_ram_sweep_profiles(
            ["32m", "64m", "128m", "256m", "512m", "1g"],
            assigned_cpu_count=10,
        )
        selected_index = 3
        select_profile(profiles, profile_index=selected_index, profiled_singleton_count=1)
        sp = get_selected_profile(profiles)
        assert sp is not None
        assert sp.resource_profile_index == selected_index

        plan = self._make_minimal_plan("test-run", [
            {
                "worker_id": "w1",
                "container_name": "worker-00001",
                "logical_client_id": "00001",
                "experiment_kind": "ram_sweep_singleton",
                "resource_profile_id": sp.resource_profile_id,
                "rayon_num_threads": sp.assigned_cpu_count,
            }
        ])

        assignments = build_worker_resource_assignments(
            run_id="test-run",
            plan=plan,
            profiles=profiles,
            selected_profile_index=selected_index,
            singleton_worker_ids=["w1"],
            singleton_client_ids=["00001"],
            singleton_container_names=["worker-00001"],
            packed_container_names=[],
            infrastructure_container_names=["ds", "relay"],
        )

        profiled_rows = [a for a in assignments if a["profile_enabled"]]
        assert len(profiled_rows) == 1
        row = profiled_rows[0]

        assert row["resource_profile_index"] == selected_index, \
            f"Expected profile index {selected_index}, got {row['resource_profile_index']}"
        assert row["resource_profile_id"] == sp.resource_profile_id, \
            f"Expected profile ID {sp.resource_profile_id}, got {row['resource_profile_id']}"
        assert row["selected_for_this_run"] is True

    def test_cpu_index_5_in_assignments(self):
        """Select CPU profile index 5; verify it appears in worker assignments.
        
        This test will FAIL before the fix because profiles[i % len(profiles)]
        always selects index 0.
        """
        profiles = generate_cpu_matrix_profiles(
            core_counts=[1, 2, 4], capacity_fractions=[0.25, 0.50, 0.75, 1.00]
        )
        selected_index = 5
        select_profile(profiles, profile_index=selected_index, profiled_singleton_count=1)
        sp = get_selected_profile(profiles)
        assert sp is not None
        assert sp.resource_profile_index == selected_index

        plan = self._make_minimal_plan("test-run", [
            {
                "worker_id": "w1",
                "container_name": "worker-00001",
                "logical_client_id": "00001",
                "experiment_kind": "cpu_matrix_singleton",
                "resource_profile_id": sp.resource_profile_id,
                "rayon_num_threads": sp.rayon_num_threads,
            }
        ])

        assignments = build_worker_resource_assignments(
            run_id="test-run",
            plan=plan,
            profiles=profiles,
            selected_profile_index=selected_index,
            singleton_worker_ids=["w1"],
            singleton_client_ids=["00001"],
            singleton_container_names=["worker-00001"],
            packed_container_names=[],
            infrastructure_container_names=["ds", "relay"],
        )

        profiled_rows = [a for a in assignments if a["profile_enabled"]]
        assert len(profiled_rows) == 1
        row = profiled_rows[0]

        assert row["resource_profile_index"] == selected_index, \
            f"Expected profile index {selected_index}, got {row['resource_profile_index']}"
        assert row["resource_profile_id"] == sp.resource_profile_id, \
            f"Expected profile ID {sp.resource_profile_id}, got {row['resource_profile_id']}"
        assert row["selected_for_this_run"] is True

    def test_parallel_assignment_includes_sweep_and_group_creator_metadata(self):
        profiles = select_all_profiles(generate_parallel_ram_sweep_profiles())
        group_creator = next(p for p in profiles if p.group_creator)
        ordinary = next(p for p in profiles if not p.group_creator)

        plan = self._make_minimal_plan("test-run", [
            {
                "worker_id": "worker-00001",
                "container_name": "worker-00001",
                "logical_client_id": "00001",
                "experiment_kind": "ram_app_heap_sweep",
                "resource_profile_id": group_creator.resource_profile_id,
                "rayon_num_threads": 1,
            },
            {
                "worker_id": "worker-00002",
                "container_name": "worker-00002",
                "logical_client_id": "00002",
                "experiment_kind": "ram_app_heap_sweep",
                "resource_profile_id": ordinary.resource_profile_id,
                "rayon_num_threads": 1,
            },
        ])

        assignments = build_worker_resource_assignments(
            run_id="test-run",
            plan=plan,
            profiles=profiles,
            selected_profile_index=0,
            singleton_worker_ids=["worker-00001", "worker-00002"],
            singleton_client_ids=["00001", "00002"],
            singleton_container_names=["worker-00001", "worker-00002"],
            packed_container_names=[],
            infrastructure_container_names=["ds", "relay"],
        )

        profiled_rows = [row for row in assignments if row["profile_enabled"]]
        creator_rows = [row for row in profiled_rows if row["group_creator"]]
        assert len(creator_rows) == 1
        assert creator_rows[0]["resource_profile_id"] == group_creator.resource_profile_id
        assert all(row["sweep_kind"] == "ram_app_heap_sweep" for row in profiled_rows)
        assert all(row["app_heap_interpretation"] for row in profiled_rows)

    def test_parallel_assignment_uses_indexed_profiles_without_affinity_assignments(self):
        profiles = select_all_profiles(generate_parallel_ram_sweep_profiles())
        plan = self._make_minimal_plan("test-run", [])

        assignments = build_worker_resource_assignments(
            run_id="test-run",
            plan=plan,
            profiles=profiles,
            selected_profile_index=0,
            singleton_worker_ids=[f"worker-{idx + 1:05d}" for idx in range(8)],
            singleton_client_ids=[f"{idx + 1:05d}" for idx in range(8)],
            singleton_container_names=[f"worker-{idx + 1:05d}" for idx in range(8)],
            packed_container_names=[],
            infrastructure_container_names=["ds", "relay"],
        )

        profiled_rows = [row for row in assignments if row["profile_enabled"]]
        profile_ids = [row["resource_profile_id"] for row in profiled_rows]

        assert profile_ids == [profile.resource_profile_id for profile in profiles]
        assert sum(1 for row in profiled_rows if row["group_creator"]) == 1
        assert all(row["selected_for_this_run"] for row in profiled_rows)

    def test_parallel_cpu_assignment_includes_cfs_quota_metadata(self):
        profiles = select_all_profiles(generate_parallel_cpu_sweep_profiles(
            [1.0, 0.75, 0.50, 0.25, 0.10, 0.05, 0.02, 0.01]
        ))
        plan = self._make_minimal_plan("test-run", [
            {
                "worker_id": f"worker-{idx + 1:05d}",
                "container_name": f"worker-{idx + 1:05d}",
                "logical_client_id": f"{idx + 1:05d}",
                "experiment_kind": "cpu_quota_sweep",
                "resource_profile_id": profile.resource_profile_id,
                "rayon_num_threads": 1,
            }
            for idx, profile in enumerate(profiles)
        ])

        assignments = build_worker_resource_assignments(
            run_id="test-run",
            plan=plan,
            profiles=profiles,
            selected_profile_index=0,
            singleton_worker_ids=[f"worker-{idx + 1:05d}" for idx in range(8)],
            singleton_client_ids=[f"{idx + 1:05d}" for idx in range(8)],
            singleton_container_names=[f"worker-{idx + 1:05d}" for idx in range(8)],
            packed_container_names=[],
            infrastructure_container_names=["ds", "relay"],
        )

        profiled_rows = [row for row in assignments if row["profile_enabled"]]
        effective = {
            round(row["cpu_quota_us"] / row["cpu_period_us"], 8)
            for row in profiled_rows
        }

        assert len(effective) == 8
        assert {row["cpu_period_us"] for row in profiled_rows} == {1_000_000}


class TestComposeGenerationPropagation:
    """Tests that nonzero selected profiles reach generated Compose config."""

    @staticmethod
    def _mock_args():
        class MockArgs:
            pass
        args = MockArgs()
        args.cpu_affinity_sample_seconds = 20.0
        args.reserve_smt_siblings = False
        return args

    @staticmethod
    def _make_mock_plan_dict(worker_container_name, resource_profile_id, cpuset_list):
        return {
            "profiled_assignments": [
                {
                    "container_name": worker_container_name,
                    "logical_client_id": "00001",
                    "assigned_cpus": cpuset_list,
                    "assigned_mask_hex": "0xf",
                    "rayon_num_threads": len(cpuset_list),
                    "experiment_kind": "ram_sweep_singleton",
                    "resource_profile_id": resource_profile_id,
                }
            ],
            "background_cpus": [4, 5, 6, 7],
            "background_mask_hex": "0xf0",
        }

    def test_ram_index_3_generates_correct_compose_limits(self):
        from generate_compose import apply_affinity_to_compose

        profiles = generate_ram_sweep_profiles(
            ["32m", "64m", "128m", "256m", "512m", "1g"],
            assigned_cpu_count=10,
        )
        selected_index = 3
        sp = select_profile(profiles, profile_index=selected_index, profiled_singleton_count=1)
        assert sp.memory_limit == "256m"

        profile_dicts = [p.to_dict() for p in profiles]
        plan = self._make_mock_plan_dict(
            worker_container_name="worker-00001",
            resource_profile_id=sp.resource_profile_id,
            cpuset_list=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        )

        lines = []
        result = apply_affinity_to_compose(
            lines, "worker-00001", "singleton",
            plan, profile_dicts, selected_index, self._mock_args(),
        )

        joined = "\n".join(lines)
        assert 'mem_limit: "256m"' in joined, \
            f"Expected mem_limit 256m, got lines: {lines}"
        assert 'memswap_limit: "256m"' in joined, \
            f"Expected memswap_limit 256m, got lines: {lines}"
        assert result.get("rayon_num_threads") == 10
        cpuset = result.get("cpuset", "")
        assert "0,1,2,3,4,5,6,7,8,9" == cpuset or cpuset == "0-9"

    def test_ram_index_0_generates_correct_compose_limits(self):
        from generate_compose import apply_affinity_to_compose

        profiles = generate_ram_sweep_profiles(
            ["32m", "64m", "128m"], assigned_cpu_count=4,
        )
        selected_index = 0
        sp = select_profile(profiles, profile_index=selected_index, profiled_singleton_count=1)
        assert sp.memory_limit == "32m"

        profile_dicts = [p.to_dict() for p in profiles]
        plan = self._make_mock_plan_dict(
            worker_container_name="worker-00001",
            resource_profile_id=sp.resource_profile_id,
            cpuset_list=[0, 1, 2, 3],
        )

        lines = []
        apply_affinity_to_compose(
            lines, "worker-00001", "singleton",
            plan, profile_dicts, selected_index, self._mock_args(),
        )

        joined = "\n".join(lines)
        assert 'mem_limit: "32m"' in joined
        assert 'memswap_limit: "32m"' in joined

    def test_cpu_index_5_generates_correct_compose_quota(self):
        from generate_compose import apply_affinity_to_compose

        profiles = generate_cpu_matrix_profiles(
            core_counts=[1, 2, 4], capacity_fractions=[0.25, 0.50, 0.75, 1.00],
        )
        selected_index = 5
        sp = select_profile(profiles, profile_index=selected_index, profiled_singleton_count=1)
        assert sp.cpu_limit_cpus is not None

        profile_dicts = [p.to_dict() for p in profiles]
        plan = self._make_mock_plan_dict(
            worker_container_name="worker-00001",
            resource_profile_id=sp.resource_profile_id,
            cpuset_list=[0, 1],
        )

        lines = []
        apply_affinity_to_compose(
            lines, "worker-00001", "singleton",
            plan, profile_dicts, selected_index, self._mock_args(),
        )

        joined = "\n".join(lines)
        expected_cpus = str(sp.cpu_limit_cpus)
        assert f'cpus: "{expected_cpus}"' in joined, \
            f"Expected cpus: {expected_cpus} in lines: {lines}"

    def test_parallel_cpu_compose_limits_without_affinity_assignments(self):
        from generate_compose import apply_affinity_to_compose

        profiles = select_all_profiles(generate_parallel_cpu_sweep_profiles())
        profile_dicts = [p.to_dict() for p in profiles]
        plan = {
            "profiled_assignments": [],
            "background_assignments": [],
        }

        lines = []
        result = apply_affinity_to_compose(
            lines,
            "worker-00015",
            "singleton",
            plan,
            profile_dicts,
            7,
            self._mock_args(),
        )

        joined = "\n".join(lines)
        assert 'cpu_quota: "10000"' in joined
        assert 'cpu_period: "1000000"' in joined
        assert result.get("resource_profile_id") == "cpu_quota_0p01"

    def test_compose_profile_matches_affinity_plan(self):
        profiles = generate_ram_sweep_profiles(
            ["32m", "64m", "128m", "256m", "512m", "1g"],
            assigned_cpu_count=10,
        )
        selected_index = 4
        sp = select_profile(profiles, profile_index=selected_index, profiled_singleton_count=1)
        assert sp.memory_limit == "512m"

        profile_dicts = [p.to_dict() for p in profiles]
        plan = self._make_mock_plan_dict(
            worker_container_name="worker-00001",
            resource_profile_id=sp.resource_profile_id,
            cpuset_list=[0, 1, 2],
        )

        from generate_compose import apply_affinity_to_compose
        lines = []
        apply_affinity_to_compose(
            lines, "worker-00001", "singleton",
            plan, profile_dicts, selected_index, self._mock_args(),
        )

        joined = "\n".join(lines)
        assert 'mem_limit: "512m"' in joined, \
            f"Expected 512m from selected profile, got: {joined}"
        assert 'mem_limit: "32m"' not in joined, \
            f"Should NOT have profile 0 limit, got: {joined}"

    def test_unprofiled_singleton_gets_only_background_cpuset(self):
        from generate_compose import apply_affinity_to_compose

        profiles = generate_ram_sweep_profiles(["32m"], assigned_cpu_count=1)
        plan = self._make_mock_plan_dict(
            worker_container_name="worker-00001",
            resource_profile_id=profiles[0].resource_profile_id,
            cpuset_list=[0],
        )
        plan["background_assignments"] = [{
            "container_name": "worker-00002",
            "assigned_cpus": [1, 2, 3],
            "assigned_mask_hex": "0xe",
        }]

        lines = []
        result = apply_affinity_to_compose(
            lines, "worker-00002", "singleton",
            plan, [p.to_dict() for p in profiles], 1, self._mock_args(),
        )

        joined = "\n".join(lines)
        assert 'cpuset: "1,2,3"' in joined
        assert "mem_limit" not in joined
        assert result.get("resource_profile_id") is None
