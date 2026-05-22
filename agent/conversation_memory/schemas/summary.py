"""Schema metadata for summary records."""

SUMMARY_KIND = "summary"
REQUIRED_FIELDS = ("summary", "covered_message_ids", "time_range")
OPTIONAL_FIELDS = (
    "keywords",
    "tags",
    "topics",
    "preferences",
    "confidence",
    "extractor_version",
)
