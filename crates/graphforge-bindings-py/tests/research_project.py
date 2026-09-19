"""Research Project metadata and bounded local discovery (#1348)."""

from __future__ import annotations

import tempfile
import uuid

import graphforge
from graphforge.exceptions import StorageError


def _open_admitted(path: str) -> graphforge.GraphForge | None:
    try:
        return graphforge.GraphForge(path)
    except StorageError as error:
        if error.code == "GF_UNSUPPORTED_FILESYSTEM":
            return None
        raise


def check_research_metadata_and_discovery() -> None:
    with tempfile.TemporaryDirectory() as first_root, tempfile.TemporaryDirectory() as second_root:
        if _open_admitted(first_root) is None or _open_admitted(second_root) is None:
            return

        metadata = {
            "contract_version": 1,
            "title": "Arabian Nights",
            "description": None,
            "authors": [],
            "subjects": ["literature"],
            "languages": ["ar"],
            "geographic_coverage": None,
            "temporal_coverage": {
                "start": "800",
                "end": "1500",
                "label": "800-1500 CE",
            },
            "source_types": [],
            "corpus_size": None,
            "ontologies": ["narrative-events"],
            "license": None,
            "access": {
                "visibility": None,
                "access_policy": None,
                "collaborators": [],
            },
            "tags": [],
            "originating_projects": [],
            "related_projects": [],
            "canonical_identifiers": [],
            "external_identifiers": [],
            "created_at": None,
            "updated_at": None,
            "extensions": {},
            "discovery_facets": {
                "text_entry_points": 0,
                "source_entry_points": 0,
                "entity_entry_points": 0,
                "relationship_entry_points": 0,
                "story_document_entry_points": 0,
                "event_location_entry_points": 0,
                "ontology_type_entry_points": 0,
                "linguistic_property_entry_points": 0,
                "traversal_query_entry_points": 0,
                "branch_entry_points": 0,
            },
        }

        forge = graphforge.GraphForge(first_root)
        forge.update_research_metadata(
            metadata=metadata,
            operation_uuid=str(uuid.uuid4()),
        )
        reopened = graphforge.GraphForge(first_root)
        assert reopened.research_project_metadata()["title"] == "Arabian Nights"
        assert reopened.research_project_summary()["metadata"]["title"] == "Arabian Nights"

        graphforge.GraphForge(second_root).update_research_metadata(
            metadata={
                **metadata,
                "title": "Medieval Latin Corpus",
                "subjects": ["history"],
                "languages": ["la"],
                "ontologies": [],
                "temporal_coverage": None,
            },
            operation_uuid=str(uuid.uuid4()),
        )

        discovered = graphforge.GraphForge.discover_research_projects(
            project_roots=[first_root, second_root],
            languages=["ar"],
            subjects=["literature"],
            ontologies=["narrative-events"],
            temporal_label="800-1500",
        )
        assert discovered.num_rows == 1
        assert discovered.column("title").to_pylist() == ["Arabian Nights"]


def main() -> None:
    check_research_metadata_and_discovery()
