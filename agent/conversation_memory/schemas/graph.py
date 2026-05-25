"""Schema metadata for knowledge graph records."""

GRAPH_KIND = "graph"
REQUIRED_FIELDS = ("entities", "relations", "triples", "covered_message_ids", "time_range")
OPTIONAL_FIELDS = ("extractor_version",)
