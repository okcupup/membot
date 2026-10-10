"""Source-checkout compatibility entrypoint for the installed membot-eval CLI."""

from membot.evaluation.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
