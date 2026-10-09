"""Compatibility wrapper for the installed single-process Worker entry point."""
import sys

from membot.service.entrypoints import main

if __name__ == "__main__":
    sys.argv.insert(1, "worker")
    main()
