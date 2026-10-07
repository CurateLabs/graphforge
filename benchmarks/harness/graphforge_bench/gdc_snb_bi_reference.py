"""Independent SNB BI reference results for the committed query fixture.

Each function below derives one BI read's expected rows directly from the
fixture CSV files with plain Python sets and dictionaries. The derivations
follow the LDBC SNB BI specification as implemented by the upstream Umbra SQL
reference queries (``umbra/queries/bi-N.sql`` in ldbc/ldbc_snb_bi), not the
Cypher text GraphForge executes, so a GraphForge result is checked against a
second, independently written formulation and never against its own output.

``python3 -m graphforge_bench.gdc_snb_bi_reference FIXTURE_DIR --write`` rewrites
``FIXTURE_DIR/expected``; without ``--write`` it fails when the committed files
differ from a fresh derivation.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
from pathlib import Path
import sys
from typing import Any

EXPECTED_SCHEMA = "graphforge-gdc-snb-bi-expected/1"
PARAMETERS_SCHEMA = "graphforge-gdc-snb-bi-query-parameters/1"

Row = list[Any]


@dataclass
class Message:
    id: int
    is_comment: bool
    creation: datetime
    content: str | None
    length: int
    language: str | None = None
    creator: int = 0
    parent: int | None = None
    tags: set[int] = field(default_factory=set)


@dataclass
class Graph:
    tag_class_name: dict[int, str]
    tag_name: dict[int, str]
    tag_class_of: dict[int, int]
    country_name: dict[int, str]
    city_name: dict[int, str]
    city_country: dict[int, int]
    person: dict[int, dict[str, Any]]
    person_city: dict[int, int]
    knows: dict[tuple[int, int], datetime]
    interests: dict[int, set[int]]
    forum: dict[int, dict[str, Any]]
    moderator: dict[int, int]
    members: dict[int, set[int]]
    container: dict[int, int]
    messages: dict[int, Message]
    likes: list[tuple[int, int]]

    def friends(self, person_id: int) -> set[int]:
        return {b for (a, b) in self.knows if a == person_id}

    def root_post(self, message_id: int) -> int:
        message = self.messages[message_id]
        while message.parent is not None:
            message = self.messages[message.parent]
        return message.id

    def forum_of(self, message_id: int) -> int:
        return self.container[self.root_post(message_id)]

    def country_of(self, person_id: int) -> int:
        return self.city_country[self.person_city[person_id]]

    def tags_named(self, name: str) -> set[int]:
        return {tag for tag, tag_name in self.tag_name.items() if tag_name == name}

    def tags_of_class(self, name: str) -> set[int]:
        return {
            tag for tag, cls in self.tag_class_of.items() if self.tag_class_name[cls] == name
        }

    def persons_in_country(self, name: str) -> set[int]:
        return {
            person
            for person in self.person
            if self.country_name[self.country_of(person)] == name
        }


def parse_datetime(text: str) -> datetime:
    return datetime.fromisoformat(text)


def render_datetime(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"


def _read(directory: Path, name: str) -> list[dict[str, str]]:
    lines = (directory / f"{name}.csv").read_text(encoding="utf-8").splitlines()
    header = lines[0].split("|")
    rows = []
    for line in lines[1:]:
        values = line.split("|")
        if len(values) != len(header):
            raise ValueError(f"{name}.csv: expected {len(header)} fields in {line!r}")
        rows.append(dict(zip(header, values, strict=True)))
    return rows


def load_graph(directory: Path) -> Graph:
    graph = Graph(
        tag_class_name={int(r["id"]): r["name"] for r in _read(directory, "TagClass")},
        tag_name={int(r["id"]): r["name"] for r in _read(directory, "Tag")},
        tag_class_of={
            int(r["TagId"]): int(r["TagClassId"])
            for r in _read(directory, "Tag_hasType_TagClass")
        },
        country_name={int(r["id"]): r["name"] for r in _read(directory, "Country")},
        city_name={int(r["id"]): r["name"] for r in _read(directory, "City")},
        city_country={
            int(r["CityId"]): int(r["CountryId"])
            for r in _read(directory, "City_isPartOf_Country")
        },
        person={
            int(r["id"]): {
                "firstName": r["firstName"],
                "lastName": r["lastName"],
                "creationDate": parse_datetime(r["creationDate"]),
            }
            for r in _read(directory, "Person")
        },
        person_city={
            int(r["PersonId"]): int(r["CityId"])
            for r in _read(directory, "Person_isLocatedIn_City")
        },
        knows={},
        interests=defaultdict(set),
        forum={
            int(r["id"]): {"title": r["title"], "creationDate": parse_datetime(r["creationDate"])}
            for r in _read(directory, "Forum")
        },
        moderator={
            int(r["ForumId"]): int(r["PersonId"])
            for r in _read(directory, "Forum_hasModerator_Person")
        },
        members=defaultdict(set),
        container={
            int(r["PostId"]): int(r["ForumId"]) for r in _read(directory, "Forum_containerOf_Post")
        },
        messages={},
        likes=[],
    )
    # Person_knows_Person is undirected: store both orientations, as the
    # Umbra reference schema does.
    for r in _read(directory, "Person_knows_Person"):
        a, b = int(r["Person1Id"]), int(r["Person2Id"])
        created = parse_datetime(r["creationDate"])
        graph.knows[(a, b)] = created
        graph.knows[(b, a)] = created
    for r in _read(directory, "Person_hasInterest_Tag"):
        graph.interests[int(r["PersonId"])].add(int(r["TagId"]))
    for r in _read(directory, "Forum_hasMember_Person"):
        graph.members[int(r["ForumId"])].add(int(r["PersonId"]))
    for r in _read(directory, "Post"):
        graph.messages[int(r["id"])] = Message(
            id=int(r["id"]),
            is_comment=False,
            creation=parse_datetime(r["creationDate"]),
            content=r["content"] or None,
            length=int(r["length"]),
            language=r["language"] or None,
        )
    for r in _read(directory, "Comment"):
        graph.messages[int(r["id"])] = Message(
            id=int(r["id"]),
            is_comment=True,
            creation=parse_datetime(r["creationDate"]),
            content=r["content"] or None,
            length=int(r["length"]),
        )
    for name, key in (
        ("Post_hasCreator_Person", "PostId"),
        ("Comment_hasCreator_Person", "CommentId"),
    ):
        for r in _read(directory, name):
            graph.messages[int(r[key])].creator = int(r["PersonId"])
    for name, key in (("Post_hasTag_Tag", "PostId"), ("Comment_hasTag_Tag", "CommentId")):
        for r in _read(directory, name):
            graph.messages[int(r[key])].tags.add(int(r["TagId"]))
    for r in _read(directory, "Comment_replyOf_Post"):
        graph.messages[int(r["CommentId"])].parent = int(r["PostId"])
    for r in _read(directory, "Comment_replyOf_Comment"):
        graph.messages[int(r["CommentId"])].parent = int(r["ParentCommentId"])
    for name, key in (("Person_likes_Post", "PostId"), ("Person_likes_Comment", "CommentId")):
        for r in _read(directory, name):
            graph.likes.append((int(r["PersonId"]), int(r[key])))
    return graph


def bi1(g: Graph, datetime: datetime) -> list[Row]:
    before = [m for m in g.messages.values() if m.creation < datetime]
    total = len(before)
    groups: dict[tuple[int, bool, int], list[int]] = defaultdict(list)
    for m in before:
        if m.content is None:
            continue
        category = 0 if m.length < 40 else 1 if m.length < 80 else 2 if m.length < 160 else 3
        groups[(m.creation.year, m.is_comment, category)].append(m.length)
    rows = []
    for (year, is_comment, category), lengths in groups.items():
        count = len(lengths)
        rows.append(
            [year, is_comment, category, count, sum(lengths) / count, sum(lengths), count / total]
        )
    rows.sort(key=lambda r: (-r[0], r[1], r[2]))
    return rows


def bi2(g: Graph, date: datetime, tagClass: str) -> list[Row]:
    w1, w2 = date + timedelta(days=100), date + timedelta(days=200)
    rows = []
    for tag in g.tags_of_class(tagClass):
        tagged = [m for m in g.messages.values() if tag in m.tags]
        c1 = sum(1 for m in tagged if date <= m.creation < w1)
        c2 = sum(1 for m in tagged if w1 <= m.creation < w2)
        rows.append([g.tag_name[tag], c1, c2, abs(c1 - c2)])
    rows.sort(key=lambda r: (-r[3], r[0]))
    return rows[:100]


def bi3(g: Graph, tagClass: str, country: str) -> list[Row]:
    class_tags = g.tags_of_class(tagClass)
    located = g.persons_in_country(country)
    counts: dict[int, int] = defaultdict(int)
    for m in g.messages.values():
        forum = g.forum_of(m.id)
        if g.moderator[forum] in located and m.tags & class_tags:
            counts[forum] += 1
    rows = [
        [
            forum,
            g.forum[forum]["title"],
            render_datetime(g.forum[forum]["creationDate"]),
            g.moderator[forum],
            count,
        ]
        for forum, count in counts.items()
    ]
    rows.sort(key=lambda r: (-r[4], r[0]))
    return rows[:20]


def bi4(g: Graph, date: datetime) -> list[Row]:
    popularity = []
    for forum, info in g.forum.items():
        if info["creationDate"] <= date:
            continue
        per_country: dict[int, int] = defaultdict(int)
        for person in g.members[forum]:
            per_country[g.country_of(person)] += 1
        if per_country:
            popularity.append((max(per_country.values()), forum))
    popularity.sort(key=lambda item: (-item[0], item[1]))
    top = {forum for _, forum in popularity[:100]}
    persons = set().union(*(g.members[forum] for forum in top)) if top else set()
    rows = []
    for person in persons:
        count = sum(
            1 for m in g.messages.values() if m.creator == person and g.forum_of(m.id) in top
        )
        info = g.person[person]
        rows.append(
            [
                person,
                info["firstName"],
                info["lastName"],
                render_datetime(info["creationDate"]),
                count,
            ]
        )
    rows.sort(key=lambda r: (-r[4], r[0]))
    return rows[:100]


def bi5(g: Graph, tag: str) -> list[Row]:
    tag_ids = g.tags_named(tag)
    replies: dict[int, int] = defaultdict(int)
    for m in g.messages.values():
        if m.parent is not None:
            replies[m.parent] += 1
    like_counts: dict[int, int] = defaultdict(int)
    for _, message in g.likes:
        like_counts[message] += 1
    stats: dict[int, list[int]] = defaultdict(lambda: [0, 0, 0])
    for m in g.messages.values():
        if m.tags & tag_ids:
            entry = stats[m.creator]
            entry[0] += replies[m.id]
            entry[1] += like_counts[m.id]
            entry[2] += 1
    rows = [
        [person, rc, lc, mc, mc + 2 * rc + 10 * lc] for person, (rc, lc, mc) in stats.items()
    ]
    rows.sort(key=lambda r: (-r[4], r[0]))
    return rows[:100]


def bi6(g: Graph, tag: str) -> list[Row]:
    tag_ids = g.tags_named(tag)
    popularity: dict[int, int] = defaultdict(int)
    for _, message in g.likes:
        popularity[g.messages[message].creator] += 1
    likers: dict[int, set[int]] = defaultdict(set)
    for m in g.messages.values():
        if m.tags & tag_ids:
            likers.setdefault(m.creator, set())
    for liker, message in g.likes:
        m = g.messages[message]
        if m.tags & tag_ids:
            likers[m.creator].add(liker)
    rows = [
        [person, sum(popularity[person2] for person2 in person2s)]
        for person, person2s in likers.items()
    ]
    rows.sort(key=lambda r: (-r[1], r[0]))
    return rows[:100]


def bi7(g: Graph, tag: str) -> list[Row]:
    tag_ids = g.tags_named(tag)
    counts: dict[str, set[int]] = defaultdict(set)
    for comment in g.messages.values():
        if comment.parent is None or comment.tags & tag_ids:
            continue
        if not g.messages[comment.parent].tags & tag_ids:
            continue
        for related in comment.tags:
            counts[g.tag_name[related]].add(comment.id)
    rows = [[name, len(comments)] for name, comments in counts.items()]
    rows.sort(key=lambda r: (-r[1], r[0]))
    return rows[:100]


def bi8(g: Graph, tag: str, startDate: datetime, endDate: datetime) -> list[Row]:
    tag_ids = g.tags_named(tag)
    score: dict[int, int] = defaultdict(int)
    for person, interests in g.interests.items():
        if interests & tag_ids:
            score[person] += 100
    for m in g.messages.values():
        if m.tags & tag_ids and startDate < m.creation < endDate:
            score[m.creator] += 1
    rows = []
    for person, own in score.items():
        friends_score = sum(score.get(friend, 0) for friend in g.friends(person))
        rows.append([person, own, friends_score])
    rows.sort(key=lambda r: (-(r[1] + r[2]), r[0]))
    return rows[:100]


def bi9(g: Graph, startDate: datetime, endDate: datetime) -> list[Row]:
    def in_window(value: datetime) -> bool:
        return startDate <= value <= endDate

    thread_messages: dict[int, int] = defaultdict(int)
    for m in g.messages.values():
        if in_window(m.creation):
            thread_messages[g.root_post(m.id)] += 1
    stats: dict[int, list[int]] = defaultdict(lambda: [0, 0])
    for post, count in thread_messages.items():
        m = g.messages[post]
        if in_window(m.creation):
            stats[m.creator][0] += 1
            stats[m.creator][1] += count
    rows = [
        [person, g.person[person]["firstName"], g.person[person]["lastName"], threads, total]
        for person, (threads, total) in stats.items()
    ]
    rows.sort(key=lambda r: (-r[4], r[0]))
    return rows[:100]


def bi10(
    g: Graph,
    personId: int,
    country: str,
    tagClass: str,
    minPathDistance: int,
    maxPathDistance: int,
) -> list[Row]:
    distance = {personId: 0}
    frontier = [personId]
    while frontier:
        following = []
        for person in frontier:
            for friend in g.friends(person):
                if friend not in distance:
                    distance[friend] = distance[person] + 1
                    following.append(friend)
        frontier = following
    located = g.persons_in_country(country)
    class_tags = g.tags_of_class(tagClass)
    counts: dict[tuple[int, str], int] = defaultdict(int)
    for m in g.messages.values():
        hops = distance.get(m.creator)
        if hops is None or not minPathDistance <= hops <= maxPathDistance:
            continue
        if m.creator not in located or not m.tags & class_tags:
            continue
        for tag in m.tags:
            counts[(m.creator, g.tag_name[tag])] += 1
    rows = [[person, name, count] for (person, name), count in counts.items()]
    rows.sort(key=lambda r: (-r[2], r[1], r[0]))
    return rows[:100]


def bi11(g: Graph, country: str, startDate: datetime, endDate: datetime) -> list[Row]:
    located = g.persons_in_country(country)

    def edge(a: int, b: int) -> bool:
        created = g.knows.get((a, b))
        return created is not None and startDate <= created <= endDate

    people = sorted(located)
    count = 0
    for i, a in enumerate(people):
        for j in range(i + 1, len(people)):
            b = people[j]
            if not edge(a, b):
                continue
            for c in people[j + 1 :]:
                if edge(b, c) and edge(c, a):
                    count += 1
    return [[count]]


def bi12(g: Graph, startDate: datetime, lengthThreshold: int, languages: list[str]) -> list[Row]:
    per_person = dict.fromkeys(g.person, 0)
    for m in g.messages.values():
        root = g.messages[g.root_post(m.id)]
        if (
            m.content is not None
            and m.length < lengthThreshold
            and m.creation > startDate
            and root.language in languages
        ):
            per_person[m.creator] += 1
    histogram: dict[int, int] = defaultdict(int)
    for count in per_person.values():
        histogram[count] += 1
    rows = [[count, persons] for count, persons in histogram.items()]
    rows.sort(key=lambda r: (-r[1], -r[0]))
    return rows


def bi13(g: Graph, country: str, endDate: datetime) -> list[Row]:
    zombies = set()
    for person in g.persons_in_country(country):
        created = g.person[person]["creationDate"]
        if created >= endDate:
            continue
        messages = sum(
            1 for m in g.messages.values() if m.creator == person and m.creation < endDate
        )
        months = 12 * (endDate.year - created.year) + (endDate.month - created.month) + 1
        if messages < months:
            zombies.add(person)
    rows = []
    for zombie in zombies:
        total = zombie_likes = 0
        for liker, message in g.likes:
            if g.messages[message].creator != zombie:
                continue
            if g.person[liker]["creationDate"] < endDate:
                total += 1
                zombie_likes += liker in zombies
        rows.append([zombie, zombie_likes, total, zombie_likes / total if total else 0.0])
    rows.sort(key=lambda r: (-r[3], r[0]))
    return rows[:100]


def bi14(g: Graph, country1: str, country2: str) -> list[Row]:
    in1, in2 = g.persons_in_country(country1), g.persons_in_country(country2)

    def replied(author: int, target: int) -> bool:
        return any(
            m.creator == author
            and m.parent is not None
            and g.messages[m.parent].creator == target
            for m in g.messages.values()
        )

    def liked(liker: int, target: int) -> bool:
        return any(
            person == liker and g.messages[message].creator == target
            for person, message in g.likes
        )

    best: dict[int, tuple[int, int, int]] = {}
    for p1, p2 in g.knows:
        if p1 not in in1 or p2 not in in2:
            continue
        score = (
            4 * replied(p1, p2) + 1 * replied(p2, p1) + 10 * liked(p1, p2) + 1 * liked(p2, p1)
        )
        city = g.person_city[p1]
        candidate = (-score, p1, p2)
        if city not in best or candidate < best[city]:
            best[city] = candidate
    rows = [
        [p1, p2, g.city_name[city], -negative] for city, (negative, p1, p2) in best.items()
    ]
    rows.sort(key=lambda r: (-r[3], r[0], r[1]))
    return rows[:100]


def bi16(
    g: Graph,
    tagA: str,
    dateA: datetime,
    tagB: str,
    dateB: datetime,
    maxKnowsLimit: int,
) -> list[Row]:
    def side(tag: str, day: datetime) -> dict[int, int]:
        tag_ids = g.tags_named(tag)
        counts: dict[int, int] = defaultdict(int)
        for m in g.messages.values():
            if m.tags & tag_ids and m.creation.date() == day.date():
                counts[m.creator] += 1
        return {
            person: count
            for person, count in counts.items()
            if len(g.friends(person) & counts.keys()) <= maxKnowsLimit
        }

    a, b = side(tagA, dateA), side(tagB, dateB)
    rows = [[person, a[person], b[person]] for person in a.keys() & b.keys()]
    rows.sort(key=lambda r: (-(r[1] + r[2]), r[0]))
    return rows[:20]


def bi17(g: Graph, tag: str, delta: int) -> list[Row]:
    tag_ids = g.tags_named(tag)
    tagged = [m for m in g.messages.values() if m.tags & tag_ids]
    found: dict[int, set[int]] = defaultdict(set)
    for message1 in tagged:
        forum1 = g.forum_of(message1.id)
        person1 = message1.creator
        for message2 in tagged:
            if not message2.creation > message1.creation + timedelta(hours=delta):
                continue
            forum2 = g.forum_of(message2.id)
            if forum1 == forum2 or person1 in g.members[forum2]:
                continue
            person3 = message2.creator
            if person3 not in g.members[forum1]:
                continue
            for comment in tagged:
                if comment.parent != message2.id:
                    continue
                person2 = comment.creator
                if person2 != person3 and person2 in g.members[forum1]:
                    found[person1].add(message2.id)
    rows = [[person, len(messages)] for person, messages in found.items()]
    rows.sort(key=lambda r: (-r[1], r[0]))
    return rows[:10]


def bi18(g: Graph, tag: str) -> list[Row]:
    tag_ids = g.tags_named(tag)
    interested = sorted(person for person, tags in g.interests.items() if tags & tag_ids)
    rows = []
    for p1 in interested:
        for p2 in interested:
            if p1 == p2 or (p1, p2) in g.knows:
                continue
            mutual = len(g.friends(p1) & g.friends(p2))
            if mutual:
                rows.append([p1, p2, mutual])
    rows.sort(key=lambda r: (-r[2], r[0], r[1]))
    return rows[:20]


DERIVATIONS = {
    "BI1": bi1,
    "BI2": bi2,
    "BI3": bi3,
    "BI4": bi4,
    "BI5": bi5,
    "BI6": bi6,
    "BI7": bi7,
    "BI8": bi8,
    "BI9": bi9,
    "BI10": bi10,
    "BI11": bi11,
    "BI12": bi12,
    "BI13": bi13,
    "BI14": bi14,
    "BI16": bi16,
    "BI17": bi17,
    "BI18": bi18,
}


def _bind(binding: dict[str, Any]) -> Any:
    kind, value = binding["kind"], binding["value"]
    if kind == "datetime":
        return parse_datetime(value)
    if kind in {"string", "int64", "string_list"}:
        return value
    raise ValueError(f"unknown parameter kind {kind}")


def load_parameters(fixture: Path) -> dict[str, dict[str, Any]]:
    document = json.loads((fixture / "parameters.json").read_text(encoding="utf-8"))
    if document.get("schema") != PARAMETERS_SCHEMA:
        raise ValueError("unexpected parameters schema")
    return document["queries"]


def derive(fixture: Path) -> dict[str, dict[str, Any]]:
    graph = load_graph(fixture / "graph")
    parameters = load_parameters(fixture)
    if set(parameters) != set(DERIVATIONS):
        raise ValueError(f"parameters must cover exactly {sorted(DERIVATIONS)}")
    expected = {}
    for operation, derivation in DERIVATIONS.items():
        bindings = {name: _bind(binding) for name, binding in parameters[operation].items()}
        expected[operation] = {
            "schema": EXPECTED_SCHEMA,
            "operation": operation,
            "authority": "independent_python_derivation_from_fixture_csv",
            "rows": derivation(graph, **bindings),
        }
    return expected


def render(document: dict[str, Any]) -> str:
    rows = ",\n".join("    " + json.dumps(row, ensure_ascii=False) for row in document["rows"])
    header = {key: value for key, value in document.items() if key != "rows"}
    head = json.dumps(header, ensure_ascii=False)[:-1]
    return f'{head}, "rows": [\n{rows}\n  ]}}\n' if rows else f'{head}, "rows": []}}\n'


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fixture", type=Path)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args(argv)
    expected_dir = args.fixture / "expected"
    stale = []
    for operation, document in derive(args.fixture).items():
        path = expected_dir / f"{operation}.json"
        text = render(document)
        if args.write:
            expected_dir.mkdir(exist_ok=True)
            path.write_text(text, encoding="utf-8")
        elif not path.is_file() or path.read_text(encoding="utf-8") != text:
            stale.append(operation)
    if stale:
        print(f"stale expected results: {', '.join(stale)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
