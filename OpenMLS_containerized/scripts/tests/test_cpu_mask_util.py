"""
Unit tests for cpu_mask_util.py
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from cpu_mask_util import (
    cpu_list_to_mask,
    mask_to_cpu_list,
    cpu_list_to_docker_cpuset,
    docker_cpuset_to_cpu_list,
    mask_to_hex,
    hex_to_mask,
    masks_overlap,
    union_masks,
    complement_mask,
    mask_popcount,
    validate_cpu_list,
    ensure_non_overlapping,
    cpu_count_from_cpuset,
)


class TestCpuListToMask:
    def test_empty_list(self):
        assert cpu_list_to_mask([]) == 0

    def test_single_cpu(self):
        assert cpu_list_to_mask([0]) == 1
        assert cpu_list_to_mask([3]) == 8

    def test_multiple_cpus(self):
        assert cpu_list_to_mask([0, 2, 4]) == 0b10101

    def test_negative_cpu_raises(self):
        with pytest.raises(ValueError, match="Negative"):
            cpu_list_to_mask([-1])

    def test_duplicate_cpu_raises(self):
        with pytest.raises(ValueError, match="Duplicate"):
            cpu_list_to_mask([0, 0])


class TestMaskToCpuList:
    def test_zero_mask(self):
        assert mask_to_cpu_list(0) == []

    def test_single_bit(self):
        assert mask_to_cpu_list(1) == [0]
        assert mask_to_cpu_list(8) == [3]

    def test_multiple_bits(self):
        assert mask_to_cpu_list(0b10101) == [0, 2, 4]

    def test_roundtrip(self):
        original = [0, 3, 7, 15]
        mask = cpu_list_to_mask(original)
        assert mask_to_cpu_list(mask) == original


class TestDockerCpusetConversion:
    def test_list_to_single(self):
        assert cpu_list_to_docker_cpuset([0]) == "0"

    def test_list_to_range(self):
        assert cpu_list_to_docker_cpuset([0, 1, 2, 3]) == "0-3"

    def test_list_to_mixed(self):
        assert cpu_list_to_docker_cpuset([0, 1, 2, 4, 5]) == "0-2,4-5"

    def test_list_to_separated(self):
        assert cpu_list_to_docker_cpuset([0, 2, 4]) == "0,2,4"

    def test_empty_list(self):
        assert cpu_list_to_docker_cpuset([]) == ""

    def test_parse_single(self):
        assert docker_cpuset_to_cpu_list("0") == [0]

    def test_parse_range(self):
        assert docker_cpuset_to_cpu_list("0-3") == [0, 1, 2, 3]

    def test_parse_mixed(self):
        assert docker_cpuset_to_cpu_list("0-2,4,5") == [0, 1, 2, 4, 5]

    def test_parse_empty(self):
        assert docker_cpuset_to_cpu_list("") == []

    def test_parse_whitespace(self):
        assert docker_cpuset_to_cpu_list(" 0-2 , 4 ") == [0, 1, 2, 4]

    def test_parse_invalid_raises(self):
        with pytest.raises(ValueError):
            docker_cpuset_to_cpu_list("abc")

    def test_parse_inverted_range_raises(self):
        with pytest.raises(ValueError):
            docker_cpuset_to_cpu_list("3-1")

    def test_roundtrip_docker(self):
        original = [0, 1, 2, 4, 5, 7]
        cpuset = cpu_list_to_docker_cpuset(original)
        assert docker_cpuset_to_cpu_list(cpuset) == original


class TestMaskHex:
    def test_mask_to_hex(self):
        assert mask_to_hex(0b10101) == "0x15"
        assert mask_to_hex(0) == "0x0"

    def test_hex_to_mask(self):
        assert hex_to_mask("0x15") == 0b10101
        assert hex_to_mask("15") == 0b10101

    def test_roundtrip(self):
        assert hex_to_mask(mask_to_hex(0b10101)) == 0b10101


class TestMaskOperations:
    def test_overlap_true(self):
        assert masks_overlap(0b0010, 0b0110) is True

    def test_overlap_false(self):
        assert masks_overlap(0b0010, 0b1100) is False

    def test_union(self):
        assert union_masks(0b0001, 0b0010, 0b0100) == 0b0111

    def test_complement(self):
        online = cpu_list_to_mask([0, 1, 2, 3, 4, 5, 6, 7])
        profiled = cpu_list_to_mask([2, 4])
        background = complement_mask(profiled, online)
        assert mask_to_cpu_list(background) == [0, 1, 3, 5, 6, 7]

    def test_popcount(self):
        assert mask_popcount(0) == 0
        assert mask_popcount(0b10101) == 3


class TestValidate:
    def test_valid_list(self):
        validate_cpu_list([0, 1, 2])

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="Empty"):
            validate_cpu_list([])

    def test_negative_raises(self):
        with pytest.raises(ValueError):
            validate_cpu_list([-1])

    def test_duplicate_raises(self):
        with pytest.raises(ValueError):
            validate_cpu_list([0, 0])


class TestNonOverlapping:
    def test_no_overlap_passes(self):
        ensure_non_overlapping([
            ("A", [0, 1]),
            ("B", [2, 3]),
        ])

    def test_overlap_raises(self):
        with pytest.raises(ValueError, match="assigned to both"):
            ensure_non_overlapping([
                ("A", [0, 1, 2]),
                ("B", [2, 3]),
            ])


class TestCpuCount:
    def test_count_single(self):
        assert cpu_count_from_cpuset("0") == 1

    def test_count_range(self):
        assert cpu_count_from_cpuset("0-3") == 4

    def test_count_mixed(self):
        assert cpu_count_from_cpuset("0-2,4,5") == 5

    def test_count_empty(self):
        assert cpu_count_from_cpuset("") == 0


class TestProfiledBackgroundExample:
    """Test the example from the spec:
    online = 0,1,2,3,4,5,6,7
    profiled = 2 and 4
    background = 0,1,3,5,6,7
    """
    def test_example(self):
        online = [0, 1, 2, 3, 4, 5, 6, 7]
        online_mask = cpu_list_to_mask(online)

        profiled_a_mask = cpu_list_to_mask([2])
        profiled_b_mask = cpu_list_to_mask([4])
        profiled_mask = union_masks(profiled_a_mask, profiled_b_mask)

        background_mask = complement_mask(profiled_mask, online_mask)
        background = mask_to_cpu_list(background_mask)

        assert mask_to_cpu_list(profiled_mask) == [2, 4]
        assert background == [0, 1, 3, 5, 6, 7]
        assert not masks_overlap(profiled_mask, background_mask)
