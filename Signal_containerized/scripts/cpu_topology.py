from __future__ import annotations

import os
from typing import List


def get_online_cpu_list(*_args, **_kwargs) -> List[int]:
    try:
        return sorted(os.sched_getaffinity(0))
    except AttributeError:
        return list(range(os.cpu_count() or 1))
