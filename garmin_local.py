from multiprocessing import freeze_support

from garmin_dashboard.analyzer import run_cli


if __name__ == "__main__":
    freeze_support()
    run_cli()
