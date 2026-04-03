from multiprocessing import freeze_support
from pathlib import Path
import sys
from time import perf_counter

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from garmin_dashboard.core.config import FIT_DIR, IntervalConfig
from garmin_dashboard.core.dataset import find_fit_files, process_batches


def main():
    files = [str(p.resolve()) for p in find_fit_files(FIT_DIR)[:300]]
    configs = [
        (1, 100),
        (2, 100),
        (4, 100),
        (6, 100),
        (8, 100),
        (4, 200),
        (6, 200),
        (8, 200),
    ]
    interval_config = IntervalConfig()
    for workers, batch in configs:
        t0 = perf_counter()
        total_rows = 0
        total_files = 0
        for batch_results in process_batches(
            files,
            batch_size=batch,
            max_workers=workers,
            interval_config=interval_config,
        ):
            total_files += len(batch_results)
            total_rows += sum(len(rows) for _, rows in batch_results)
        dt = perf_counter() - t0
        print(f"workers={workers} batch={batch} files={total_files} rows={total_rows} seconds={dt:.3f}")


if __name__ == "__main__":
    freeze_support()
    main()
