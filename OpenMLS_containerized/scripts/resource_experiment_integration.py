"""
Resource experiment integration module for run_compose_benchmark.py.

Provides functions to be called at key points in the benchmark workflow:
  - build_affinity_plan_before_compose(): Build and write affinity plan, resource profiles
  - run_preflight_after_startup(): Run preflight checks after container startup
  - write_results_after_run(): Write failure/timeline/run-status sidecars after the run
  - classify_and_write_failures(): Classify failures and write sidecar files
"""

import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from cpu_mask_util import (
    cpu_list_to_mask,
    cpu_list_to_docker_cpuset,
    mask_to_hex,
    mask_to_cpu_list,
)
from cpu_topology import detect_cpu_topology, get_online_cpu_list
from cpu_affinity_planner import (
    create_affinity_plan,
    write_affinity_plan_json,
    get_background_cpuset,
    get_profiled_cpuset,
    get_rayon_num_threads,
    validate_affinity_plan,
    AffinityPlan,
)
from resource_profiles import (
    ResourceProfile,
    generate_ram_sweep_profiles,
    generate_cpu_matrix_profiles,
    generate_embedded_budget_profiles,
    get_selected_profile,
    select_profile,
)
from resource_experiment_sidecars import SidecarWriter
from resource_experiment_failures import (
    WorkerFailureInfo,
    classify_worker_failure,
    classify_worker_failure_from_resource_summary,
    build_run_status,
    worker_failure_info_to_dict,
    check_oom_events_file,
    FAILURE_CLASSES,
)
from preflight_checks import run_preflight


class ResourceExperimentIntegration:
    """Handles all resource experiment integration for a benchmark run."""

    def __init__(
        self,
        run_id: str,
        run_dir: str,
        resource_experiment: str = "none",
        profiled_singleton_count: int = 1,
        ram_sweep_values: Optional[List[str]] = None,
        ram_sweep_cpu_count: int = 10,
        cpu_matrix_core_counts: Optional[List[int]] = None,
        cpu_matrix_capacity_fractions: Optional[List[float]] = None,
        embedded_heap_budgets: Optional[List[str]] = None,
        embedded_cpu_fractions: Optional[List[float]] = None,
        embedded_cpu_cores: Optional[List[int]] = None,
        embedded_docker_memory: str = "256m",
        ram_app_heap_sweep_values: Optional[List[str]] = None,
        cpu_affinity_mode: str = "none",
        cpu_affinity_sample_seconds: float = 20.0,
        reserve_smt_siblings: bool = False,
    ):
        self.run_id = run_id
        self.run_dir = run_dir
        self.resource_experiment = resource_experiment
        self.profiled_singleton_count = profiled_singleton_count
        self.ram_sweep_values = ram_sweep_values or ["32m", "64m", "128m", "256m", "512m", "1g"]
        self.ram_sweep_cpu_count = ram_sweep_cpu_count
        self.cpu_matrix_core_counts = cpu_matrix_core_counts or [1, 2, 4]
        self.cpu_matrix_capacity_fractions = cpu_matrix_capacity_fractions or [0.25, 0.50, 0.75, 1.00]
        self.embedded_heap_budgets = embedded_heap_budgets or ["32k", "64k", "128k", "256k", "512k", "1m", "2m"]
        self.embedded_cpu_fractions = embedded_cpu_fractions or [1.00, 0.50, 0.25, 0.10, 0.05]
        self.embedded_cpu_cores = embedded_cpu_cores or [1]
        self.embedded_docker_memory = embedded_docker_memory
        self.ram_app_heap_sweep_values = ram_app_heap_sweep_values or []
        self.cpu_affinity_mode = cpu_affinity_mode
        self.cpu_affinity_sample_seconds = cpu_affinity_sample_seconds
        self.reserve_smt_siblings = reserve_smt_siblings

        if self.resource_experiment != "none" and self.cpu_affinity_mode == "none":
            self.cpu_affinity_mode = "profiled-nor-background"

        self.plan: Optional[AffinityPlan] = None
        self.profiles: List[ResourceProfile] = []
        self.writer = SidecarWriter(run_dir)
        self.compose_output_dir: Optional[str] = None

    @property
    def is_active(self) -> bool:
        return self.resource_experiment in (
            "ram-sweep-singleton",
            "cpu-matrix-singleton",
            "embedded-budget-singleton",
            "ram-app-heap-sweep",
            "cpu-quota-sweep",
        )

    @property
    def experiment_kind(self) -> str:
        if self.resource_experiment == "ram-sweep-singleton":
            return "ram_sweep_singleton"
        elif self.resource_experiment == "cpu-matrix-singleton":
            return "cpu_matrix_singleton"
        elif self.resource_experiment == "embedded-budget-singleton":
            return "embedded_budget_singleton"
        elif self.resource_experiment == "ram-app-heap-sweep":
            return "ram_app_heap_sweep"
        elif self.resource_experiment == "cpu-quota-sweep":
            return "cpu_quota_sweep"
        return "none"

    def generate_profiles(self) -> List[ResourceProfile]:
        """Generate resource profiles for the experiment."""
        if self.resource_experiment == "ram-sweep-singleton":
            self.profiles = generate_ram_sweep_profiles(
                ram_values=self.ram_sweep_values,
                assigned_cpu_count=self.ram_sweep_cpu_count,
                run_id=self.run_id,
            )
        elif self.resource_experiment == "cpu-matrix-singleton":
            self.profiles = generate_cpu_matrix_profiles(
                core_counts=self.cpu_matrix_core_counts,
                capacity_fractions=self.cpu_matrix_capacity_fractions,
                run_id=self.run_id,
            )
        elif self.resource_experiment == "embedded-budget-singleton":
            self.profiles = generate_embedded_budget_profiles(
                heap_budgets=self.embedded_heap_budgets,
                core_counts=self.embedded_cpu_cores,
                capacity_fractions=self.embedded_cpu_fractions,
                docker_memory_limit=self.embedded_docker_memory,
                run_id=self.run_id,
            )
        elif self.resource_experiment == "ram-app-heap-sweep":
            heap_budgets = self.ram_app_heap_sweep_values if self.ram_app_heap_sweep_values else None
            self.profiles = generate_parallel_ram_sweep_profiles(
                heap_budgets=heap_budgets,
                docker_memory_limit=self.embedded_docker_memory,
                assigned_cpu_count=1,
                run_id=self.run_id,
            )
        elif self.resource_experiment == "cpu-quota-sweep":
            self.profiles = generate_parallel_cpu_sweep_profiles(
                assigned_cpu_count=1,
                run_id=self.run_id,
            )
        return self.profiles

    def build_affinity_plan(
        self,
        singleton_worker_ids: List[str],
        singleton_client_ids: List[str],
        background_containers: List[Dict[str, str]],
    ) -> AffinityPlan:
        """Build the CPU affinity plan."""
        self.generate_profiles()

        profiled_worker_specs = []
        profiled_cpu_counts = {}

        for i, (worker_id, client_id) in enumerate(
            zip(singleton_worker_ids[:self.profiled_singleton_count],
                singleton_client_ids[:self.profiled_singleton_count])
        ):
            profile = get_selected_profile(self.profiles) if self.profiles else None
            if profile is None and self.profiles:
                profile = self.profiles[i % len(self.profiles)]
            cpu_count = profile.assigned_cpu_count if profile else 1

            profiled_worker_specs.append({
                "worker_id": worker_id,
                "container_name": f"worker-{client_id}",
                "logical_client_id": client_id,
                "experiment_kind": self.experiment_kind,
                "resource_profile_id": profile.resource_profile_id if profile else "",
            })
            profiled_cpu_counts[worker_id] = cpu_count

        bg_specs = []
        for bc in background_containers:
            bg_specs.append({
                "container_name": bc["container_name"],
                "container_role": bc.get("container_role", "background"),
            })

        self.plan = create_affinity_plan(
            run_id=self.run_id,
            profiled_worker_specs=profiled_worker_specs,
            background_specs=bg_specs,
            cpu_affinity_mode=self.cpu_affinity_mode,
            sample_seconds=self.cpu_affinity_sample_seconds,
            reserve_smt_siblings=self.reserve_smt_siblings,
            profiled_cpu_counts=profiled_cpu_counts,
        )

        selected = get_selected_profile(self.profiles) if self.profiles else None
        for i, pa in enumerate(self.plan.profiled_assignments):
            profile = selected if (i == 0 and selected) else (self.profiles[i] if i < len(self.profiles) else None)
            if profile:
                profile.cpuset_cpus = cpu_list_to_docker_cpuset(pa.assigned_cpus)
                profile.cpuset_mask_hex = pa.assigned_mask_hex
                profile.rayon_num_threads = pa.rayon_num_threads
                profile.assigned_cpu_count = pa.assigned_cpu_count

        return self.plan

    def write_affinity_and_profile_files(self, compose_output_dir: str):
        """Write affinity plan and resource profiles to files for the compose generator."""
        self.compose_output_dir = compose_output_dir
        os.makedirs(compose_output_dir, exist_ok=True)

        if self.plan:
            plan_path = os.path.join(compose_output_dir, "cpu_affinity_plan.json")
            write_affinity_plan_json(self.plan, compose_output_dir)
        else:
            plan_path = ""

        profiles_path = ""
        if self.profiles:
            profiles_path = os.path.join(compose_output_dir, "resource_profiles.json")
            profiles_data = [p.to_dict() for p in self.profiles]
            with open(profiles_path, "w") as f:
                json.dump(profiles_data, f, indent=2)

        self.writer.write_resource_profiles(
            self.run_id, [p.to_dict() for p in self.profiles]
        )

        return plan_path, profiles_path

    def run_preflight(
        self,
        profiled_container_names: List[str],
        background_container_names: List[str],
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """Run preflight checks after container startup."""
        profiled_containers = []
        for name in profiled_container_names:
            cpuset = get_profiled_cpuset(self.plan, name) if self.plan else ""
            rayon = get_rayon_num_threads(self.plan, name) if self.plan else 0
            profiled_containers.append({
                "container_name": name,
                "container_role": "profiled_singleton",
                "expected_cpuset": cpuset or "",
                "expected_rayon_threads": rayon or 0,
            })

        background_containers = []
        bg_cpuset = get_background_cpuset(self.plan) if self.plan else ""
        for name in background_container_names:
            background_containers.append({
                "container_name": name,
                "container_role": "background",
                "expected_cpuset": bg_cpuset,
            })

        results, all_passed = run_preflight(
            run_id=self.run_id,
            profiled_containers=profiled_containers,
            background_containers=background_containers,
            affinity_plan=self.plan,
        )

        self.writer.write_preflight_results(self.run_id, results)
        return results, all_passed

    def write_failure_results(
        self,
        run_success: bool,
        worker_failures: Optional[List[WorkerFailureInfo]] = None,
        resource_summaries: Optional[List[Dict[str, Any]]] = None,
        notes: str = "",
    ):
        """Write failure, run status, and resource summary sidecars."""
        if worker_failures is None:
            worker_failures = []

        if resource_summaries:
            self.writer.write_resource_summary(self.run_id, resource_summaries)

            for rs in resource_summaries:
                klass, info = classify_worker_failure_from_resource_summary(
                    rs, os.path.join(self.run_dir, "oom_events.jsonl")
                )
                info.failure_class = klass
                if klass != "completed_successfully":
                    worker_failures.append(info)

        if not worker_failures and run_success:
            worker_failures = []

        failure_dicts = [worker_failure_info_to_dict(wf) for wf in worker_failures]
        if failure_dicts:
            self.writer.write_worker_failures(self.run_id, failure_dicts)

        selected_profile = get_selected_profile(self.profiles) if self.profiles else None

        run_status = build_run_status(
            run_id=self.run_id,
            run_mode=self.resource_experiment,
            experiment_kind=self.experiment_kind,
            run_success=run_success,
            worker_failures=worker_failures,
            resource_experiment=self.resource_experiment,
            memory_model=getattr(selected_profile, "memory_model", "") if selected_profile else "",
            docker_memory_limit=(
                getattr(selected_profile, "docker_memory_limit", None)
                or getattr(selected_profile, "memory_limit", "")
            ) if selected_profile else "",
            app_heap_budget=getattr(selected_profile, "app_heap_budget", "") if selected_profile else "",
            app_heap_budget_bytes=getattr(selected_profile, "app_heap_budget_bytes", 0) if selected_profile else 0,
            notes=notes,
        )
        self.writer.write_run_status(self.run_id, run_status)

    def write_worker_resource_assignments(
        self,
        singleton_worker_ids: List[str],
        singleton_client_ids: List[str],
        singleton_container_names: List[str],
        packed_container_names: List[str],
        infrastructure_container_names: List[str],
    ):
        """Write worker_resource_assignments.csv."""
        from resource_experiment_runner import build_worker_resource_assignments

        assignments = build_worker_resource_assignments(
            run_id=self.run_id,
            plan=self.plan,
            profiles=self.profiles,
            singleton_worker_ids=singleton_worker_ids[:self.profiled_singleton_count],
            singleton_client_ids=singleton_client_ids[:self.profiled_singleton_count],
            singleton_container_names=singleton_container_names[:self.profiled_singleton_count],
            packed_container_names=packed_container_names,
            infrastructure_container_names=infrastructure_container_names,
        )

        self.writer.write_worker_resource_assignments(self.run_id, assignments)
        return assignments


def add_resource_experiment_args(parser):
    """Add resource experiment CLI arguments to an argparse parser."""
    re_group = parser.add_argument_group("Resource Experiment Options")

    re_group.add_argument(
        "--resource-experiment",
        choices=[
            "none",
            "ram-sweep-singleton",
            "cpu-matrix-singleton",
            "embedded-budget-singleton",
            "ram-app-heap-sweep",
            "cpu-quota-sweep",
        ],
        default="none",
        help="Resource experiment mode for simulated IoT container profiling",
    )
    re_group.add_argument(
        "--embedded-heap-budgets",
        default="32k,64k,128k,256k,512k,1m,2m",
        help="Comma-separated synthetic application heap budgets for embedded-budget mode",
    )
    re_group.add_argument(
        "--embedded-cpu-fractions",
        default="1.00,0.50,0.25,0.10,0.05",
        help="Comma-separated Docker CPU fractions for embedded-budget mode",
    )
    re_group.add_argument(
        "--embedded-cpu-cores",
        default="1",
        help="Comma-separated Docker CPU core counts for embedded-budget mode",
    )
    re_group.add_argument(
        "--embedded-docker-memory",
        default="256m",
        help="Safe Docker memory limit used while Rust enforces app heap budget",
    )
    re_group.add_argument(
        "--ram-app-heap-sweep-values",
        default="",
        help="Comma-separated list of 10 application heap budgets for ram-app-heap-sweep",
    )
    re_group.add_argument(
        "--profiled-singleton-count",
        type=int,
        default=1,
        help="Number of profiled singleton workers in resource experiment mode",
    )
    re_group.add_argument(
        "--ram-sweep-values",
        default="32m,64m,128m,256m,512m,1g",
        help="Comma-separated list of Docker memory limits for RAM sweep",
    )
    re_group.add_argument(
        "--ram-sweep-cpu-count",
        type=int,
        default=10,
        help="Number of CPUs assigned to each singleton in RAM sweep",
    )
    re_group.add_argument(
        "--cpu-matrix-core-counts",
        default="1,2,4",
        help="Comma-separated list of CPU core counts for CPU matrix",
    )
    re_group.add_argument(
        "--cpu-matrix-capacity-fractions",
        default="0.25,0.50,0.75,1.00",
        help="Comma-separated list of capacity fractions for CPU matrix",
    )
    re_group.add_argument(
        "--cpu-affinity-mode",
        choices=["none", "profiled-nor-background"],
        default="none",
        help="CPU affinity planning mode",
    )
    re_group.add_argument(
        "--cpu-affinity-sample-seconds",
        type=float,
        default=20.0,
        help="Duration in seconds for CPU load sampling (0 or 1 for smoke tests)",
    )
    re_group.add_argument(
        "--reserve-smt-siblings",
        action="store_true",
        help="Reserve SMT/hyperthread siblings of profiled CPUs",
    )

    return parser


def parse_resource_experiment_args(args) -> ResourceExperimentIntegration:
    """Parse resource experiment args from an argparse Namespace and return an integration object."""
    run_dir = os.path.join(
        getattr(args, "output_dir", "benchmark_output"),
        getattr(args, "run_id", "unknown"),
    )

    ram_values = getattr(args, "ram_sweep_values", "32m,64m,128m,256m,512m,1g")
    if isinstance(ram_values, str):
        ram_values = [v.strip() for v in ram_values.split(",") if v.strip()]

    cpu_cores = getattr(args, "cpu_matrix_core_counts", "1,2,4")
    if isinstance(cpu_cores, str):
        cpu_cores = [int(v.strip()) for v in cpu_cores.split(",") if v.strip()]

    cpu_fractions = getattr(args, "cpu_matrix_capacity_fractions", "0.25,0.50,0.75,1.00")
    if isinstance(cpu_fractions, str):
        cpu_fractions = [float(v.strip()) for v in cpu_fractions.split(",") if v.strip()]

    embedded_heap_budgets = getattr(args, "embedded_heap_budgets", "32k,64k,128k,256k,512k,1m,2m")
    if isinstance(embedded_heap_budgets, str):
        embedded_heap_budgets = [v.strip() for v in embedded_heap_budgets.split(",") if v.strip()]

    embedded_cpu_fractions = getattr(args, "embedded_cpu_fractions", "1.00,0.50,0.25,0.10,0.05")
    if isinstance(embedded_cpu_fractions, str):
        embedded_cpu_fractions = [float(v.strip()) for v in embedded_cpu_fractions.split(",") if v.strip()]

    embedded_cpu_cores = getattr(args, "embedded_cpu_cores", "1")
    if isinstance(embedded_cpu_cores, str):
        embedded_cpu_cores = [int(v.strip()) for v in embedded_cpu_cores.split(",") if v.strip()]

    ram_app_heap_sweep_values = getattr(args, "ram_app_heap_sweep_values", "") or ""
    ram_app_heap_sweep_values = (
        [v.strip() for v in ram_app_heap_sweep_values.split(",") if v.strip()]
        if ram_app_heap_sweep_values else []
    )

    return ResourceExperimentIntegration(
        run_id=getattr(args, "run_id", "unknown"),
        run_dir=run_dir,
        resource_experiment=getattr(args, "resource_experiment", "none"),
        profiled_singleton_count=getattr(args, "profiled_singleton_count", 1),
        ram_sweep_values=ram_values,
        ram_sweep_cpu_count=getattr(args, "ram_sweep_cpu_count", 10),
        cpu_matrix_core_counts=cpu_cores,
        cpu_matrix_capacity_fractions=cpu_fractions,
        embedded_heap_budgets=embedded_heap_budgets,
        embedded_cpu_fractions=embedded_cpu_fractions,
        embedded_cpu_cores=embedded_cpu_cores,
        embedded_docker_memory=getattr(args, "embedded_docker_memory", "256m"),
        ram_app_heap_sweep_values=ram_app_heap_sweep_values,
        cpu_affinity_mode=getattr(args, "cpu_affinity_mode", "none"),
        cpu_affinity_sample_seconds=getattr(args, "cpu_affinity_sample_seconds", 20.0),
        reserve_smt_siblings=getattr(args, "reserve_smt_siblings", False),
    )
