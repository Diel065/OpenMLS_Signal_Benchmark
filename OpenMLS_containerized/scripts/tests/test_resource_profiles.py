"""
Unit tests for resource_profiles.py
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from resource_profiles import (
    ResourceProfile,
    generate_ram_sweep_profiles,
    generate_cpu_matrix_profiles,
    validate_memory_string,
    parse_memory_to_bytes,
    profile_to_compose_dict,
)


class TestRamSweepProfiles:
    def test_generates_correct_count(self):
        profiles = generate_ram_sweep_profiles(
            ram_values=["32m", "64m", "128m"],
            assigned_cpu_count=10,
        )
        assert len(profiles) == 3

    def test_max_6_values(self):
        with pytest.raises(ValueError, match="at most 6"):
            generate_ram_sweep_profiles(
                ram_values=["1m", "2m", "3m", "4m", "5m", "6m", "7m"],
            )

    def test_experiment_kind(self):
        profiles = generate_ram_sweep_profiles(
            ram_values=["64m"],
            assigned_cpu_count=10,
        )
        assert profiles[0].experiment_kind == "ram_sweep_singleton"

    def test_memory_swap_equals_memory_limit(self):
        profiles = generate_ram_sweep_profiles(
            ram_values=["128m"],
            assigned_cpu_count=10,
        )
        assert profiles[0].memory_swap == "128m"
        assert profiles[0].memory_limit == "128m"

    def test_rayon_equals_assigned_cpu_count(self):
        profiles = generate_ram_sweep_profiles(
            ram_values=["64m"],
            assigned_cpu_count=10,
        )
        assert profiles[0].rayon_num_threads == 10
        assert profiles[0].assigned_cpu_count == 10

    def test_cpu_quota_unset(self):
        profiles = generate_ram_sweep_profiles(
            ram_values=["64m"],
            assigned_cpu_count=10,
        )
        assert profiles[0].cpu_limit_cpus is None

    def test_capacity_fraction_unset(self):
        profiles = generate_ram_sweep_profiles(ram_values=["64m"])
        assert profiles[0].capacity_fraction is None

    def test_profile_id_format(self):
        profiles = generate_ram_sweep_profiles(ram_values=["128m"])
        assert "ram_128m" in profiles[0].resource_profile_id or "128m" in profiles[0].resource_profile_id

    def test_default_values(self):
        profiles = generate_ram_sweep_profiles(
            ram_values=["32m", "64m", "128m", "256m", "512m", "1g"],
            assigned_cpu_count=10,
        )
        assert len(profiles) == 6
        for p in profiles:
            assert p.memory_swap == p.memory_limit


class TestCpuMatrixProfiles:
    def test_generates_12_cells(self):
        profiles = generate_cpu_matrix_profiles(
            core_counts=[1, 2, 4],
            capacity_fractions=[0.25, 0.50, 0.75, 1.00],
        )
        assert len(profiles) == 12

    def test_experiment_kind(self):
        profiles = generate_cpu_matrix_profiles(
            core_counts=[1],
            capacity_fractions=[0.5],
        )
        assert profiles[0].experiment_kind == "cpu_matrix_singleton"

    def test_cpu_quota_calculation(self):
        profiles = generate_cpu_matrix_profiles(
            core_counts=[1, 2, 4],
            capacity_fractions=[0.25, 0.50, 0.75, 1.00],
        )

        expected_quotas = {
            (1, 0.25): 0.25, (1, 0.50): 0.50, (1, 0.75): 0.75, (1, 1.00): 1.00,
            (2, 0.25): 0.50, (2, 0.50): 1.00, (2, 0.75): 1.50, (2, 1.00): 2.00,
            (4, 0.25): 1.00, (4, 0.50): 2.00, (4, 0.75): 3.00, (4, 1.00): 4.00,
        }

        for p in profiles:
            key = (p.assigned_cpu_count, p.capacity_fraction)
            assert key in expected_quotas
            assert p.cpu_limit_cpus == pytest.approx(expected_quotas[key])

    def test_rayon_equals_assigned_cpu_count(self):
        profiles = generate_cpu_matrix_profiles(
            core_counts=[2],
            capacity_fractions=[0.5],
        )
        assert profiles[0].rayon_num_threads == 2

    def test_no_impossible_combinations(self):
        profiles = generate_cpu_matrix_profiles(
            core_counts=[1],
            capacity_fractions=[0.25, 0.5, 2.0],
        )
        quotas = [p.cpu_limit_cpus for p in profiles]
        for q in quotas:
            assert q <= 2.0

    def test_high_memory_set(self):
        profiles = generate_cpu_matrix_profiles(
            core_counts=[1],
            capacity_fractions=[0.5],
            high_memory_limit="2g",
        )
        assert profiles[0].memory_limit == "2g"
        assert profiles[0].memory_swap == "2g"


class TestMemoryValidation:
    def test_valid_strings(self):
        assert validate_memory_string("128m") is True
        assert validate_memory_string("1g") is True
        assert validate_memory_string("64k") is True
        assert validate_memory_string("1024b") is True

    def test_invalid_strings(self):
        assert validate_memory_string("abc") is False
        assert validate_memory_string("128") is False
        assert validate_memory_string("128x") is False

    def test_empty_ok(self):
        assert validate_memory_string("") is True
        assert validate_memory_string(None) is True


class TestMemoryParsing:
    def test_parse_m(self):
        assert parse_memory_to_bytes("1m") == 1024 * 1024
        assert parse_memory_to_bytes("128m") == 128 * 1024 * 1024

    def test_parse_g(self):
        assert parse_memory_to_bytes("1g") == 1024 * 1024 * 1024

    def test_parse_k(self):
        assert parse_memory_to_bytes("64k") == 64 * 1024

    def test_parse_b(self):
        assert parse_memory_to_bytes("1024b") == 1024

    def test_parse_invalid(self):
        assert parse_memory_to_bytes("abc") == -1
        assert parse_memory_to_bytes("") == -1
        assert parse_memory_to_bytes(None) == -1


class TestToComposeDict:
    def test_ram_sweep_profile(self):
        p = ResourceProfile(
            resource_profile_id="test",
            experiment_kind="ram_sweep_singleton",
            profile_label="test",
            assigned_cpu_count=10,
            memory_limit="128m",
            memory_swap="128m",
            rayon_num_threads=10,
            cpuset_cpus="0-9",
        )
        d = profile_to_compose_dict(p)
        assert d["cpuset"] == "0-9"
        assert d["mem_limit"] == "128m"
        assert d["memswap_limit"] == "128m"
        assert "cpus" not in d

    def test_cpu_matrix_profile(self):
        p = ResourceProfile(
            resource_profile_id="test",
            experiment_kind="cpu_matrix_singleton",
            profile_label="test",
            assigned_cpu_count=2,
            cpu_limit_cpus=1.0,
            capacity_fraction=0.5,
            rayon_num_threads=2,
            cpuset_cpus="4-5",
        )
        d = profile_to_compose_dict(p)
        assert d["cpuset"] == "4-5"
        assert d["cpus"] == "1.0"


class TestResourceProfileToDict:
    def test_all_fields_present(self):
        p = ResourceProfile(
            resource_profile_id="rp-1",
            experiment_kind="ram_sweep_singleton",
            profile_label="RAM=128m CPUs=10",
            memory_limit="128m",
            memory_swap="128m",
            assigned_cpu_count=10,
            rayon_num_threads=10,
        )
        d = p.to_dict()
        for key in ["resource_profile_id", "experiment_kind", "profile_label",
                     "cpu_limit_cpus", "capacity_fraction", "assigned_cpu_count",
                     "memory_limit", "memory_swap", "rayon_num_threads",
                     "cpuset_cpus", "cpuset_mask_hex", "profile_notes"]:
            assert key in d
