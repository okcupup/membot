"""Compatibility wrapper for the installed single-process API entry point."""
import sys

from membot.service.entrypoints import main  # noqa: F401

if __name__ == "__main__":
    sys.argv.insert(1, "api")
    main()
