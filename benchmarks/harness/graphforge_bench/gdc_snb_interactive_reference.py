"""Independent reference for the SNB Interactive v1 query fixture.

This module builds the committed synthetic SNB Interactive fixture and derives
each read query's expected result from the fixture data in plain Python. It
never calls GraphForge: the expected rows are computed from the LDBC SNB
Interactive v1 query semantics (the pinned specification and the Cypher
reference implementation at the pinned driver commit) so that the Rust runner's
live GraphForge output is checked against an answer derived another way.

Values follow the Cypher reference implementation's data model: dates and
datetimes are epoch milliseconds (UTC), ``email`` and ``speaks`` are string
lists, and every node has one label (a Message is a Post or a Comment).
This is #952 decision 2026-10-07: Interactive follows the v1 reference data
model (epoch-ms Int64).

Regenerate the committed files with::

    PYTHONPATH=benchmarks/harness python3 -m graphforge_bench.gdc_snb_interactive_reference \
        --write benchmarks/fixtures/gdc/snb-interactive-queries
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
import json
import math
from pathlib import Path
from typing import Any

DATASET_ID = "snb-interactive-query-synthetic-v1"
FIXTURE_SCHEMA = "graphforge-gdc-snb-interactive-query-fixture/1"
EXPECTED_SCHEMA = "graphforge-gdc-snb-interactive-query-expected/1"

DAY = 86_400_000
MINUTE = 60_000
BASE = 1_262_304_000_000  # 2010-01-01T00:00:00Z

Row = list[Any]


class _Lcg:
    """Deterministic 64-bit LCG; the fixture must not depend on Python's RNG."""

    def __init__(self, seed: int) -> None:
        self.state = seed

    def next(self) -> int:
        self.state = (self.state * 6364136223846793005 + 1442695040888963407) % (1 << 64)
        return self.state >> 33

    def below(self, bound: int) -> int:
        return self.next() % bound

    def choice(self, values: list[Any]) -> Any:
        return values[self.below(len(values))]

    def sample(self, values: list[Any], count: int) -> list[Any]:
        pool = list(values)
        picked = []
        for _ in range(min(count, len(pool))):
            picked.append(pool.pop(self.below(len(pool))))
        return picked


def _epoch_millis(year: int, month: int, day: int) -> int:
    return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp() * 1000)


# --------------------------------------------------------------------------
# Fixture construction
# --------------------------------------------------------------------------

PERSON_COUNT = 50
CONNECTED_PERSONS = 45  # persons 45..49 form a separate KNOWS component
POST_COUNT = 200
COMMENT_COUNT = 400
LIKE_COUNT = 500
FORUM_COUNT = 24

COUNTRIES = [
    (9010, "Avalon", 9000),
    (9011, "Borduria", 9000),
    (9012, "Carpania", 9001),
    (9013, "Dalmora", 9001),
    (9014, "Elbonia", 9000),
]
TAG_CLASSES = [
    (9400, "Thing", None),
    (9401, "Agent", 9400),
    (9402, "Artist", 9401),
    (9403, "MusicalArtist", 9402),
    (9404, "Place", 9400),
    (9405, "Work", 9400),
    (9406, "Album", 9405),
]
TAG_WORDS = [
    "Alpha",
    "Bravo",
    "Charlie",
    "Delta",
    "Echo",
    "Foxtrot",
    "Golf",
    "Hotel",
    "India",
    "Juliett",
    "Kilo",
    "Lima",
    "Mike",
    "November",
    "Oscar",
    "Papa",
    "Quebec",
    "Romeo",
    "Sierra",
    "Tango",
    "Uniform",
    "Victor",
    "Whiskey",
    "Xray",
    "Yankee",
    "Zulu",
    "Amber",
    "Basalt",
    "Cobalt",
    # A Tag named like a TagClass exercises the reference IC12 predicate
    # `tag.name = $tagClassName OR baseTagClass.name = $tagClassName`.
    "Artist",
]
FIRST_NAMES = ["Ana", "Ken", "Mei", "Omar", "Lena", "Ravi"]
LAST_NAMES = ["Garcia", "Smith", "Wong", "Ivanov", "Okafor", "Silva"]
BROWSERS = ["Firefox", "Chrome", "Safari", "Opera"]
LANGUAGES = ["en", "es", "zh", "ru", "pt"]


class _Builder:
    def __init__(self) -> None:
        self.nodes: list[dict[str, Any]] = []
        self.edges: list[dict[str, Any]] = []

    def node(self, label: str, node_id: int, properties: dict[str, Any]) -> str:
        key = f"{label}:{node_id}"
        self.nodes.append({"key": key, "label": label, "properties": {"id": node_id, **properties}})
        return key

    def edge(
        self,
        source: str,
        rel_type: str,
        destination: str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        self.edges.append(
            {
                "source": source,
                "type": rel_type,
                "destination": destination,
                "properties": properties or {},
            }
        )


def build_fixture() -> dict[str, Any]:
    """Build the deterministic synthetic SNB-shaped graph."""
    rng = _Lcg(1880)
    out = _Builder()

    continents = {9000: "Europe", 9001: "Asia"}
    for continent_id, name in continents.items():
        out.node("Continent", continent_id, {"name": name})
    country_keys: list[str] = []
    city_keys: list[str] = []
    for index, (country_id, name, continent_id) in enumerate(COUNTRIES):
        country = out.node("Country", country_id, {"name": name})
        country_keys.append(country)
        out.edge(country, "IS_PART_OF", f"Continent:{continent_id}")
        for offset, suffix in enumerate(("North", "South")):
            city = out.node("City", 9100 + 2 * index + offset, {"name": f"{name} {suffix}"})
            city_keys.append(city)
            out.edge(city, "IS_PART_OF", country)

    universities = []
    for index in range(4):
        university = out.node("University", 9200 + index, {"name": f"University {index}"})
        out.edge(university, "IS_LOCATED_IN", city_keys[(3 * index) % len(city_keys)])
        universities.append(university)
    companies = []
    for index in range(8):
        company = out.node("Company", 9300 + index, {"name": f"Company {chr(65 + index)}"})
        out.edge(company, "IS_LOCATED_IN", country_keys[index % len(country_keys)])
        companies.append(company)

    for class_id, name, _parent in TAG_CLASSES:
        out.node("TagClass", class_id, {"name": name})
    for class_id, _name, parent in TAG_CLASSES:
        if parent is not None:
            out.edge(f"TagClass:{class_id}", "IS_SUBCLASS_OF", f"TagClass:{parent}")
    tags = []
    for index, word in enumerate(TAG_WORDS):
        tag = out.node("Tag", 9500 + index, {"name": word})
        tag_class = 9404 if word == "Artist" else TAG_CLASSES[index % len(TAG_CLASSES)][0]
        out.edge(tag, "HAS_TYPE", f"TagClass:{tag_class}")
        tags.append(tag)

    persons: list[str] = []
    person_created: list[int] = []
    for index in range(PERSON_COUNT):
        person_id = 2000 + (index * 37) % PERSON_COUNT
        if rng.below(10) < 6:
            # Concentrate birthdays in the IC10 window for month 5.
            month, day = (5, 15 + rng.below(17)) if rng.below(2) == 0 else (6, 1 + rng.below(28))
        else:
            month, day = 1 + rng.below(12), 1 + rng.below(28)
        created = BASE + index * DAY + rng.below(DAY)
        emails = [f"p{person_id}@mail{k}.example" for k in range(1 + rng.below(2))]
        person = out.node(
            "Person",
            person_id,
            {
                "firstName": "Jose" if index % 2 == 0 else rng.choice(FIRST_NAMES),
                "lastName": rng.choice(LAST_NAMES),
                "gender": rng.choice(["female", "male"]),
                "birthday": _epoch_millis(1980 + rng.below(10), month, day),
                "creationDate": created,
                "locationIP": f"10.0.{index}.{rng.below(255)}",
                "browserUsed": rng.choice(BROWSERS),
                "email": emails,
                "speaks": rng.sample(LANGUAGES, 1 + rng.below(2)),
            },
        )
        persons.append(person)
        person_created.append(created)
        out.edge(person, "IS_LOCATED_IN", rng.choice(city_keys))
        for university in rng.sample(universities, rng.below(3)):
            out.edge(person, "STUDY_AT", university, {"classYear": 2000 + rng.below(15)})
        for company in rng.sample(companies, rng.below(4)):
            out.edge(person, "WORK_AT", company, {"workFrom": 1998 + rng.below(16)})
        for tag in rng.sample(tags, 1 + rng.below(4)):
            out.edge(person, "HAS_INTEREST", tag)

    knows: set[tuple[int, int]] = set()
    for index in range(1, CONNECTED_PERSONS):
        # A random recursive tree keeps one component with long and short paths.
        knows.add((rng.below(index), index))
    while len(knows) < CONNECTED_PERSONS - 1 + 30:
        first, second = rng.below(CONNECTED_PERSONS), rng.below(CONNECTED_PERSONS)
        if first != second:
            knows.add((min(first, second), max(first, second)))
    for index in range(CONNECTED_PERSONS, PERSON_COUNT - 1):
        knows.add((index, index + 1))
    for order, (first, second) in enumerate(sorted(knows)):
        created = max(person_created[first], person_created[second]) + rng.below(30) * DAY
        out.edge(
            persons[first], "KNOWS", persons[second], {"creationDate": created + order * MINUTE}
        )

    forums: list[str] = []
    forum_members: list[list[int]] = []
    for index in range(FORUM_COUNT):
        created = BASE + 60 * DAY + index * 7 * DAY
        forum = out.node(
            "Forum", 7000 + index, {"title": f"Group {index}", "creationDate": created}
        )
        forums.append(forum)
        out.edge(forum, "HAS_MODERATOR", rng.choice(persons))
        members = rng.sample(list(range(PERSON_COUNT)), 5 + rng.below(12))
        forum_members.append(members)
        for member in members:
            joined = created + rng.below(400) * DAY + rng.below(DAY)
            out.edge(forum, "HAS_MEMBER", persons[member], {"joinDate": joined})
        for tag in rng.sample(tags, 1 + rng.below(3)):
            out.edge(forum, "HAS_TAG", tag)

    messages: list[tuple[str, int, int]] = []  # (key, creationDate, creator index)
    message_ids: set[int] = set()

    def message_id(sequence: int) -> int:
        identifier = 100_000 + (sequence * 7919) % 99_991
        assert identifier not in message_ids
        message_ids.add(identifier)
        return identifier

    sequence = 0
    for _ in range(POST_COUNT):
        sequence += 1
        forum_index = rng.below(FORUM_COUNT)
        creator = rng.choice(forum_members[forum_index])
        created = BASE + 200 * DAY + rng.below(500) * DAY + rng.below(DAY)
        identifier = message_id(sequence)
        properties: dict[str, Any] = {
            "creationDate": created,
            "locationIP": f"10.1.{sequence % 250}.{rng.below(255)}",
            "browserUsed": rng.choice(BROWSERS),
            "length": 0,
        }
        if rng.below(4) == 0:
            properties["imageFile"] = f"photo{identifier}.jpg"
        else:
            properties["content"] = f"post {identifier} text"
            properties["language"] = rng.choice(LANGUAGES)
            properties["length"] = len(properties["content"])
        post = out.node("Post", identifier, properties)
        out.edge(post, "HAS_CREATOR", persons[creator])
        out.edge(forums[forum_index], "CONTAINER_OF", post)
        out.edge(post, "IS_LOCATED_IN", rng.choice(country_keys))
        for tag in rng.sample(tags, 1 + rng.below(4)):
            out.edge(post, "HAS_TAG", tag)
        messages.append((post, created, creator))

    for _ in range(COMMENT_COUNT):
        sequence += 1
        if rng.below(5) == 0:
            # Concentrate replies on one person's messages so IC8 exceeds its LIMIT.
            parent_key, parent_created, _parent_creator = rng.choice(
                [message for message in messages if message[2] == 0]
            )
        else:
            parent_key, parent_created, _parent_creator = rng.choice(messages)
        creator = rng.below(PERSON_COUNT)
        created = parent_created + 1 + rng.below(20 * DAY)
        identifier = message_id(sequence)
        content = f"comment {identifier} text"
        comment = out.node(
            "Comment",
            identifier,
            {
                "creationDate": created,
                "locationIP": f"10.2.{sequence % 250}.{rng.below(255)}",
                "browserUsed": rng.choice(BROWSERS),
                "content": content,
                "length": len(content),
            },
        )
        out.edge(comment, "HAS_CREATOR", persons[creator])
        out.edge(comment, "REPLY_OF", parent_key)
        out.edge(comment, "IS_LOCATED_IN", rng.choice(country_keys))
        for tag in rng.sample(tags, rng.below(3)):
            out.edge(comment, "HAS_TAG", tag)
        messages.append((comment, created, creator))

    likes: set[tuple[int, int]] = set()
    while len(likes) < LIKE_COUNT:
        likes.add((rng.below(PERSON_COUNT), rng.below(len(messages))))
    for liker, message_index in sorted(likes):
        message_key, created, _creator = messages[message_index]
        like_time = created + rng.below(5000) * MINUTE + rng.below(MINUTE)
        out.edge(persons[liker], "LIKES", message_key, {"creationDate": like_time})

    # Ids are unique per entity type only. An unconnected University sharing a
    # Post id makes the (m:Post OR m:Comment) label test observable in IS4.
    out.node("University", 155_433, {"name": "Unconnected University"})
    _add_ties(out)
    return {
        "schema": FIXTURE_SCHEMA,
        "dataset_id": DATASET_ID,
        "classification": "synthetic_engineering_fixture",
        # Compact rows: [key, label, properties] and
        # [source, type, destination, properties].
        "nodes": [[node["key"], node["label"], node["properties"]] for node in out.nodes],
        "edges": [
            [edge["source"], edge["type"], edge["destination"], edge["properties"]]
            for edge in out.edges
        ],
    }


def _add_ties(out: _Builder) -> None:
    """Make tie-breaking sort keys observable for selected parameters."""
    nodes = {node["key"]: node for node in out.nodes}
    for tied_key, source_key in TIED_MESSAGES:
        nodes[tied_key]["properties"]["creationDate"] = nodes[source_key]["properties"][
            "creationDate"
        ]
    for liker, (first, second) in TIED_LIKES.items():
        times = [
            edge["properties"]
            for edge in out.edges
            if edge["type"] == "LIKES"
            and edge["source"] == liker
            and edge["destination"] in (first, second)
        ]
        assert len(times) == 2, (liker, first, second)
        latest = max(times[0]["creationDate"], times[1]["creationDate"])
        times[0]["creationDate"] = times[1]["creationDate"] = latest
    knows = {
        frozenset((edge["source"], edge["destination"])): edge["properties"]
        for edge in out.edges
        if edge["type"] == "KNOWS"
    }
    for tied, source in TIED_KNOWS:
        knows[frozenset(tied)]["creationDate"] = knows[frozenset(source)]["creationDate"]


# Each pair makes two sort keys equal so the query's tie-breaker decides the
# order: IS2 (message id), IC2 and IC9 (message id), IS3 (person id) and IC7
# (lowest message id among a liker's simultaneous latest likes).
TIED_MESSAGES: list[tuple[str, str]] = [
    ("Post:162294", "Post:114671"),
    ("Comment:192803", "Post:137479"),
    ("Comment:163756", "Post:183935"),
]
TIED_LIKES: dict[str, tuple[str, str]] = {"Person:2029": ("Post:176016", "Post:198933")}
TIED_KNOWS: list[tuple[tuple[str, str], tuple[str, str]]] = [
    (("Person:2009", "Person:2018"), ("Person:2009", "Person:2023")),
]


# --------------------------------------------------------------------------
# Independent evaluation
# --------------------------------------------------------------------------


class Graph:
    """Read-only index over the fixture document."""

    def __init__(self, document: dict[str, Any]) -> None:
        self.nodes: dict[str, dict[str, Any]] = {}
        self.labels: dict[str, set[str]] = {}
        for key, label, properties in document["nodes"]:
            self.nodes[key] = properties
            self.labels[key] = {label}
        self._with_label: dict[str, list[str]] = {}
        self._by_id: dict[str, dict[int, str]] = {}
        self.out: dict[tuple[str, str], list[tuple[str, dict[str, Any]]]] = {}
        self.into: dict[tuple[str, str], list[tuple[str, dict[str, Any]]]] = {}
        for source, rel_type, destination, props in document["edges"]:
            self.out.setdefault((source, rel_type), []).append((destination, props))
            self.into.setdefault((destination, rel_type), []).append((source, props))

    def with_label(self, label: str) -> list[str]:
        # The graph is read-only, so each label's node list is computed once;
        # a scorecard rung evaluates many bindings over millions of nodes.
        cached = self._with_label.get(label)
        if cached is None:
            cached = [key for key, labels in self.labels.items() if label in labels]
            self._with_label[label] = cached
        return cached

    def by_id(self, label: str, node_id: int) -> str | None:
        index = self._by_id.get(label)
        if index is None:
            index = {}
            for key in self.with_label(label):
                identity = self.nodes[key]["id"]
                assert identity not in index, (label, identity)
                index[identity] = key
            self._by_id[label] = index
        return index.get(node_id)

    def outgoing(self, key: str, rel_type: str) -> list[tuple[str, dict[str, Any]]]:
        return self.out.get((key, rel_type), [])

    def incoming(self, key: str, rel_type: str) -> list[tuple[str, dict[str, Any]]]:
        return self.into.get((key, rel_type), [])

    def one_out(self, key: str, rel_type: str) -> str:
        targets = self.outgoing(key, rel_type)
        assert len(targets) == 1, (key, rel_type)
        return targets[0][0]

    def prop(self, key: str, name: str) -> Any:
        return self.nodes[key].get(name)

    def knows(self, key: str) -> list[tuple[str, dict[str, Any]]]:
        return self.outgoing(key, "KNOWS") + self.incoming(key, "KNOWS")

    def distances(self, start: str, limit: int | None = None) -> dict[str, int]:
        dist = {start: 0}
        frontier = [start]
        while frontier and (limit is None or dist[frontier[0]] < limit):
            following = []
            for key in frontier:
                for other, _props in self.knows(key):
                    if other not in dist:
                        dist[other] = dist[key] + 1
                        following.append(other)
            frontier = following
        return dist

    def within(self, start: str, low: int, high: int) -> list[str]:
        return [key for key, d in self.distances(start, high).items() if low <= d <= high]

    def creator(self, message: str) -> str:
        return self.one_out(message, "HAS_CREATOR")

    def messages_by(self, person: str) -> list[str]:
        return [message for message, _props in self.incoming(person, "HAS_CREATOR")]

    def root_post(self, message: str) -> str:
        while "Post" not in self.labels[message]:
            message = self.one_out(message, "REPLY_OF")
        return message


def _first_non_null(*values: Any) -> Any:
    return next((value for value in values if value is not None), None)


def _person_id(graph: Graph, params: dict[str, Any], name: str = "personId") -> str:
    person = graph.by_id("Person", params[name])
    assert person is not None, params
    return person


def _person_summary(graph: Graph, person: str) -> Row:
    return [
        graph.prop(person, "id"),
        graph.prop(person, "firstName"),
        graph.prop(person, "lastName"),
    ]


def ic1(graph: Graph, params: dict[str, Any]) -> list[Row]:
    start = _person_id(graph, params)
    found = [
        (distance, key)
        for key, distance in graph.distances(start, 3).items()
        if distance >= 1 and graph.prop(key, "firstName") == params["firstName"]
    ]
    found.sort(
        key=lambda item: (item[0], graph.prop(item[1], "lastName"), graph.prop(item[1], "id"))
    )
    rows = []
    for distance, friend in found[:20]:
        # The specification's <name, year, place> tuples, keyed by field name.
        universities = [
            {
                "name": graph.prop(university, "name"),
                "classYear": study["classYear"],
                "city": graph.prop(graph.one_out(university, "IS_LOCATED_IN"), "name"),
            }
            for university, study in graph.outgoing(friend, "STUDY_AT")
        ]
        companies = [
            {
                "name": graph.prop(company, "name"),
                "workFrom": work["workFrom"],
                "country": graph.prop(graph.one_out(company, "IS_LOCATED_IN"), "name"),
            }
            for company, work in graph.outgoing(friend, "WORK_AT")
        ]
        # Reference behaviour, differs from spec prose: the reference's
        # `CASE uni.name WHEN null THEN null ELSE [...] END` compares with `=`
        # and never matches (Neo4j semantics), so the OPTIONAL MATCH's single
        # all-null row is collected as an all-null tuple, not dropped.
        if not universities:
            universities = [{"name": None, "classYear": None, "city": None}]
        if not companies:
            companies = [{"name": None, "workFrom": None, "country": None}]
        rows.append(
            [
                graph.prop(friend, "id"),
                graph.prop(friend, "lastName"),
                distance,
                graph.prop(friend, "birthday"),
                graph.prop(friend, "creationDate"),
                graph.prop(friend, "gender"),
                graph.prop(friend, "browserUsed"),
                graph.prop(friend, "locationIP"),
                graph.prop(friend, "email"),
                graph.prop(friend, "speaks"),
                graph.prop(graph.one_out(friend, "IS_LOCATED_IN"), "name"),
                universities,
                companies,
            ]
        )
    return rows


def _message_rows(graph: Graph, people: Iterable[str], keep: Callable[[int], bool]) -> list[Row]:
    rows = []
    for person in people:
        for message in graph.messages_by(person):
            created = graph.prop(message, "creationDate")
            if keep(created):
                rows.append(
                    [
                        *_person_summary(graph, person),
                        graph.prop(message, "id"),
                        _first_non_null(
                            graph.prop(message, "content"), graph.prop(message, "imageFile")
                        ),
                        created,
                    ]
                )
    rows.sort(key=lambda row: (-row[5], row[3]))
    return rows[:20]


def ic2(graph: Graph, params: dict[str, Any]) -> list[Row]:
    start = _person_id(graph, params)
    friends = graph.within(start, 1, 1)
    return _message_rows(graph, friends, lambda created: created <= params["maxDate"])


def ic3(graph: Graph, params: dict[str, Any]) -> list[Row]:
    start = _person_id(graph, params)
    country_x = next(
        key
        for key in graph.with_label("Country")
        if graph.prop(key, "name") == params["countryXName"]
    )
    country_y = next(
        key
        for key in graph.with_label("Country")
        if graph.prop(key, "name") == params["countryYName"]
    )
    excluded_cities = {
        city
        for country in (country_x, country_y)
        for city, _props in graph.incoming(country, "IS_PART_OF")
        if "City" in graph.labels[city]
    }
    rows = []
    for friend in graph.within(start, 1, 2):
        if graph.one_out(friend, "IS_LOCATED_IN") in excluded_cities:
            continue
        x_count = y_count = 0
        for message in graph.messages_by(friend):
            created = graph.prop(message, "creationDate")
            if not params["startDate"] <= created < params["endDate"]:
                continue
            country = graph.one_out(message, "IS_LOCATED_IN")
            x_count += country == country_x
            y_count += country == country_y
        if x_count > 0 and y_count > 0:
            rows.append([*_person_summary(graph, friend), x_count, y_count, x_count + y_count])
    rows.sort(key=lambda row: (-row[5], row[0]))
    return rows[:20]


def ic4(graph: Graph, params: dict[str, Any]) -> list[Row]:
    start = _person_id(graph, params)
    valid: dict[str, int] = {}
    invalid: dict[str, int] = {}
    for friend in graph.within(start, 1, 1):
        for post in graph.messages_by(friend):
            if "Post" not in graph.labels[post]:
                continue
            created = graph.prop(post, "creationDate")
            for tag, _props in graph.outgoing(post, "HAS_TAG"):
                valid.setdefault(tag, 0)
                invalid.setdefault(tag, 0)
                valid[tag] += params["startDate"] <= created < params["endDate"]
                invalid[tag] += created < params["startDate"]
    rows = [
        [graph.prop(tag, "name"), count]
        for tag, count in valid.items()
        if count > 0 and invalid[tag] == 0
    ]
    rows.sort(key=lambda row: (-row[1], row[0]))
    return rows[:10]


def ic5(graph: Graph, params: dict[str, Any]) -> list[Row]:
    start = _person_id(graph, params)
    others = set(graph.within(start, 1, 2))
    rows = []
    for forum in graph.with_label("Forum"):
        joined = {
            person
            for person, membership in graph.outgoing(forum, "HAS_MEMBER")
            if person in others and membership["joinDate"] > params["minDate"]
        }
        if not joined:
            continue
        posts = sum(
            1
            for post, _props in graph.outgoing(forum, "CONTAINER_OF")
            if graph.creator(post) in joined
        )
        rows.append((posts, graph.prop(forum, "id"), graph.prop(forum, "title")))
    rows.sort(key=lambda row: (-row[0], row[1]))
    return [[title, posts] for posts, _forum_id, title in rows[:20]]


def ic6(graph: Graph, params: dict[str, Any]) -> list[Row]:
    start = _person_id(graph, params)
    counts: dict[str, int] = {}
    for other in graph.within(start, 1, 2):
        for post in graph.messages_by(other):
            if "Post" not in graph.labels[post]:
                continue
            names = [graph.prop(tag, "name") for tag, _props in graph.outgoing(post, "HAS_TAG")]
            if params["tagName"] not in names:
                continue
            for name in names:
                if name != params["tagName"]:
                    counts[name] = counts.get(name, 0) + 1
    rows = [[name, count] for name, count in counts.items()]
    rows.sort(key=lambda row: (-row[1], row[0]))
    return rows[:10]


def ic7(graph: Graph, params: dict[str, Any]) -> list[Row]:
    start = _person_id(graph, params)
    friends = {other for other, _props in graph.knows(start)}
    latest: dict[str, tuple[int, int, str]] = {}
    for message in graph.messages_by(start):
        for liker, like in graph.incoming(message, "LIKES"):
            candidate = (like["creationDate"], graph.prop(message, "id"), message)
            current = latest.get(liker)
            # Most recent like; equal times resolve to the lowest message id.
            if current is None or (candidate[0], -candidate[1]) > (current[0], -current[1]):
                latest[liker] = candidate
    rows = []
    for liker, (like_time, _message_id, message) in latest.items():
        latency = like_time - graph.prop(message, "creationDate")
        rows.append(
            [
                *_person_summary(graph, liker),
                like_time,
                graph.prop(message, "id"),
                _first_non_null(graph.prop(message, "content"), graph.prop(message, "imageFile")),
                # toInteger(floor(toFloat(latency) / 1000.0) / 60.0): floor to
                # seconds, then truncate the minutes toward zero.
                int(math.floor(float(latency) / 1000.0) / 60.0),
                liker not in friends,
            ]
        )
    rows.sort(key=lambda row: (-row[3], row[0]))
    return rows[:20]


def ic8(graph: Graph, params: dict[str, Any]) -> list[Row]:
    start = _person_id(graph, params)
    rows = []
    for message in graph.messages_by(start):
        for comment, _props in graph.incoming(message, "REPLY_OF"):
            rows.append(
                [
                    *_person_summary(graph, graph.creator(comment)),
                    graph.prop(comment, "creationDate"),
                    graph.prop(comment, "id"),
                    graph.prop(comment, "content"),
                ]
            )
    rows.sort(key=lambda row: (-row[3], row[4]))
    return rows[:20]


def ic9(graph: Graph, params: dict[str, Any]) -> list[Row]:
    start = _person_id(graph, params)
    others = graph.within(start, 1, 2)
    return _message_rows(graph, others, lambda created: created < params["maxDate"])


def ic10(graph: Graph, params: dict[str, Any]) -> list[Row]:
    start = _person_id(graph, params)
    interests = {tag for tag, _props in graph.outgoing(start, "HAS_INTEREST")}
    month = params["month"]
    following = month % 12 + 1
    rows = []
    for candidate in graph.within(start, 2, 2):
        birthday = datetime.fromtimestamp(graph.prop(candidate, "birthday") / 1000, tz=timezone.utc)
        if not (
            (birthday.month == month and birthday.day >= 21)
            or (birthday.month == following and birthday.day < 22)
        ):
            continue
        posts = [m for m in graph.messages_by(candidate) if "Post" in graph.labels[m]]
        common = sum(
            1
            for post in posts
            if any(tag in interests for tag, _props in graph.outgoing(post, "HAS_TAG"))
        )
        rows.append(
            [
                *_person_summary(graph, candidate),
                common - (len(posts) - common),
                graph.prop(candidate, "gender"),
                graph.prop(graph.one_out(candidate, "IS_LOCATED_IN"), "name"),
            ]
        )
    rows.sort(key=lambda row: (-row[3], row[0]))
    return rows[:10]


def ic11(graph: Graph, params: dict[str, Any]) -> list[Row]:
    start = _person_id(graph, params)
    rows = []
    for other in graph.within(start, 1, 2):
        for company, work in graph.outgoing(other, "WORK_AT"):
            country = graph.one_out(company, "IS_LOCATED_IN")
            if (
                graph.prop(country, "name") == params["countryName"]
                and work["workFrom"] < params["workFromYear"]
            ):
                rows.append(
                    [*_person_summary(graph, other), graph.prop(company, "name"), work["workFrom"]]
                )
    rows.sort(key=lambda row: row[3], reverse=True)
    rows.sort(key=lambda row: (row[4], row[0]))
    return rows[:10]


def _tag_class_closure(graph: Graph, tag_class: str) -> set[str]:
    seen = {tag_class}
    frontier = [tag_class]
    while frontier:
        following = []
        for key in frontier:
            for parent, _props in graph.outgoing(key, "IS_SUBCLASS_OF"):
                if parent not in seen:
                    seen.add(parent)
                    following.append(parent)
        frontier = following
    return seen


def ic12(graph: Graph, params: dict[str, Any]) -> list[Row]:
    start = _person_id(graph, params)
    name = params["tagClassName"]
    selected = set()
    for tag in graph.with_label("Tag"):
        bases = {
            base
            for tag_class, _props in graph.outgoing(tag, "HAS_TYPE")
            for base in _tag_class_closure(graph, tag_class)
        }
        if bases and (
            graph.prop(tag, "name") == name
            or any(graph.prop(base, "name") == name for base in bases)
        ):
            selected.add(tag)
    rows = []
    for friend in graph.within(start, 1, 1):
        comments = set()
        tag_names = set()
        for comment in graph.messages_by(friend):
            if "Comment" not in graph.labels[comment]:
                continue
            for post, _props in graph.outgoing(comment, "REPLY_OF"):
                if "Post" not in graph.labels[post]:
                    continue
                for tag, _tag_props in graph.outgoing(post, "HAS_TAG"):
                    if tag in selected:
                        comments.add(comment)
                        tag_names.add(graph.prop(tag, "name"))
        if comments:
            rows.append([*_person_summary(graph, friend), sorted(tag_names), len(comments)])
    rows.sort(key=lambda row: (-row[4], row[0]))
    return rows[:20]


def ic13(graph: Graph, params: dict[str, Any]) -> list[Row]:
    first = _person_id(graph, params, "person1Id")
    second = _person_id(graph, params, "person2Id")
    return [[graph.distances(first).get(second, -1)]]


def is1(graph: Graph, params: dict[str, Any]) -> list[Row]:
    person = _person_id(graph, params)
    return [
        [
            graph.prop(person, "firstName"),
            graph.prop(person, "lastName"),
            graph.prop(person, "birthday"),
            graph.prop(person, "locationIP"),
            graph.prop(person, "browserUsed"),
            graph.prop(graph.one_out(person, "IS_LOCATED_IN"), "id"),
            graph.prop(person, "gender"),
            graph.prop(person, "creationDate"),
        ]
    ]


def is2(graph: Graph, params: dict[str, Any]) -> list[Row]:
    person = _person_id(graph, params)
    recent = sorted(
        graph.messages_by(person),
        key=lambda m: (-graph.prop(m, "creationDate"), graph.prop(m, "id")),
    )[:10]
    rows = []
    for message in recent:
        post = graph.root_post(message)
        rows.append(
            [
                graph.prop(message, "id"),
                _first_non_null(graph.prop(message, "imageFile"), graph.prop(message, "content")),
                graph.prop(message, "creationDate"),
                graph.prop(post, "id"),
                *_person_summary(graph, graph.creator(post)),
            ]
        )
    return rows


def is3(graph: Graph, params: dict[str, Any]) -> list[Row]:
    person = _person_id(graph, params)
    rows = [
        [*_person_summary(graph, friend), knows["creationDate"]]
        for friend, knows in graph.knows(person)
    ]
    rows.sort(key=lambda row: (-row[3], row[0]))
    return rows


def _message_id(graph: Graph, params: dict[str, Any]) -> str:
    message = graph.by_id("Post", params["messageId"]) or graph.by_id(
        "Comment", params["messageId"]
    )
    assert message is not None, params
    return message


def is4(graph: Graph, params: dict[str, Any]) -> list[Row]:
    message = _message_id(graph, params)
    return [
        [
            graph.prop(message, "creationDate"),
            _first_non_null(graph.prop(message, "content"), graph.prop(message, "imageFile")),
        ]
    ]


def is5(graph: Graph, params: dict[str, Any]) -> list[Row]:
    return [_person_summary(graph, graph.creator(_message_id(graph, params)))]


def is6(graph: Graph, params: dict[str, Any]) -> list[Row]:
    post = graph.root_post(_message_id(graph, params))
    rows = []
    for forum, _props in graph.incoming(post, "CONTAINER_OF"):
        moderator = graph.one_out(forum, "HAS_MODERATOR")
        rows.append(
            [
                graph.prop(forum, "id"),
                graph.prop(forum, "title"),
                *_person_summary(graph, moderator),
            ]
        )
    return rows


def is7(graph: Graph, params: dict[str, Any]) -> list[Row]:
    message = _message_id(graph, params)
    rows = []
    for comment, _props in graph.incoming(message, "REPLY_OF"):
        replier = graph.creator(comment)
        rows.append(
            [
                graph.prop(comment, "id"),
                graph.prop(comment, "content"),
                graph.prop(comment, "creationDate"),
                *_person_summary(graph, replier),
                # Reference behaviour, differs from spec prose: the reference's
                # `CASE r WHEN null THEN false ELSE true END` never matches null
                # (Neo4j semantics), so the knows flag is always true.
                True,
            ]
        )
    rows.sort(key=lambda row: (-row[2], row[3]))
    return rows


EVALUATORS: dict[str, Callable[[Graph, dict[str, Any]], list[Row]]] = {
    "IC1": ic1,
    "IC2": ic2,
    "IC3": ic3,
    "IC4": ic4,
    "IC5": ic5,
    "IC6": ic6,
    "IC7": ic7,
    "IC8": ic8,
    "IC9": ic9,
    "IC10": ic10,
    "IC11": ic11,
    "IC12": ic12,
    "IC13": ic13,
    "IS1": is1,
    "IS2": is2,
    "IS3": is3,
    "IS4": is4,
    "IS5": is5,
    "IS6": is6,
    "IS7": is7,
}

# Parameter bindings per operation, chosen to give non-trivial results.
PARAMETERS: dict[str, list[dict[str, Any]]] = {
    "IC1": [{"personId": 2048, "firstName": "Jose"}, {"personId": 2009, "firstName": "Ana"}],
    # maxDate equals a friend's message creationDate: the reference keeps it (<=).
    "IC2": [{"personId": 2037, "maxDate": 1_301_069_492_890}],
    "IC3": [
        {
            "personId": 2020,
            "countryXName": "Avalon",
            "countryYName": "Borduria",
            "startDate": 1_280_000_000_000,
            "endDate": 1_306_000_000_000,
        }
    ],
    "IC4": [{"personId": 2022, "startDate": 1_290_000_000_000, "endDate": 1_300_000_000_000}],
    "IC5": [{"personId": 2048, "minDate": 1_288_224_000_000}],
    "IC6": [{"personId": 2048, "tagName": "Alpha"}],
    "IC7": [{"personId": 2008}],
    "IC8": [{"personId": 2000}],
    # maxDate equals a message creationDate: the reference excludes it (<).
    "IC9": [{"personId": 2048, "maxDate": 1_301_475_031_846}],
    "IC10": [{"personId": 2048, "month": 5}, {"personId": 2022, "month": 12}],
    "IC11": [
        {"personId": 2029, "countryName": "Avalon", "workFromYear": 2010},
        {"personId": 2037, "countryName": "Carpania", "workFromYear": 2014},
    ],
    "IC12": [{"personId": 2009, "tagClassName": "Artist"}],
    "IC13": [
        {"person1Id": 2048, "person2Id": 2021},
        {"person1Id": 2048, "person2Id": 2048},
        {"person1Id": 2048, "person2Id": 2039},
    ],
    "IS1": [{"personId": 2048}],
    "IS2": [{"personId": 2023}],
    "IS3": [{"personId": 2009}],
    "IS4": [{"messageId": 155_433}, {"messageId": 155_215}],
    "IS5": [{"messageId": 155_215}],
    "IS6": [{"messageId": 155_215}, {"messageId": 122_699}],
    "IS7": [{"messageId": 122_699}],
}


def expected_document(document: dict[str, Any]) -> dict[str, Any]:
    """Derive every expected result from the fixture document."""
    graph = Graph(document)
    return {
        "schema": EXPECTED_SCHEMA,
        "dataset_id": DATASET_ID,
        "derivation": "graphforge_bench.gdc_snb_interactive_reference (independent of GraphForge)",
        "operations": {
            operation: [
                {"parameters": params, "rows": EVALUATORS[operation](graph, params)}
                for params in PARAMETERS[operation]
            ]
            for operation in EVALUATORS
        },
    }


def _compact(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"))


def render_graph(document: dict[str, Any]) -> str:
    """Render the fixture with one node or edge per line."""
    header = {key: value for key, value in document.items() if key not in ("nodes", "edges")}
    lines = ["{"]
    lines.extend(f"{_compact(key)}:{_compact(value)}," for key, value in header.items())
    for name in ("nodes", "edges"):
        rows = document[name]
        lines.append(f'"{name}":[')
        lines.extend(
            _compact(row) + ("," if index + 1 < len(rows) else "") for index, row in enumerate(rows)
        )
        lines.append("]," if name == "nodes" else "]")
    lines.append("}")
    return "\n".join(lines) + "\n"


def render_expected(document: dict[str, Any]) -> str:
    """Render expected results with one row per line."""
    lines = ["{"]
    lines.extend(
        f"{_compact(key)}:{_compact(document[key])},"
        for key in ("schema", "dataset_id", "derivation")
    )
    lines.append('"operations":{')
    operations = list(document["operations"].items())
    for op_index, (operation, cases) in enumerate(operations):
        lines.append(f"{_compact(operation)}:[")
        for case_index, case in enumerate(cases):
            lines.append(f'{{"parameters":{_compact(case["parameters"])},"rows":[')
            rows = case["rows"]
            lines.extend(
                _compact(row) + ("," if index + 1 < len(rows) else "")
                for index, row in enumerate(rows)
            )
            lines.append("]}" + ("," if case_index + 1 < len(cases) else ""))
        lines.append("]" + ("," if op_index + 1 < len(operations) else ""))
    lines.append("}")
    lines.append("}")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", type=Path, required=True, help="fixture directory to write")
    args = parser.parse_args(argv)
    document = build_fixture()
    args.write.mkdir(parents=True, exist_ok=True)
    (args.write / "graph.json").write_text(render_graph(document), encoding="utf-8")
    (args.write / "expected.json").write_text(
        render_expected(expected_document(document)), encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
