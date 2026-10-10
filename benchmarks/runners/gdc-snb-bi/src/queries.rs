//! Runnable LDBC SNB BI read queries (#1879).
//!
//! Every analytical read this suite maps is defined here as data: the
//! operation, the Cypher text GraphForge executes, the parameter names a driver
//! must bind, and the result columns in order. A driver iterates
//! [`BI_QUERIES`]; reads the public surface cannot express exactly are in
//! [`REFUSED_READS`] with a typed cause and are never approximated.
//!
//! The texts follow the LDBC SNB BI reference implementation at
//! [`UPSTREAM_QUERY_COMMIT`] (`neo4j/queries/bi-N.cypher`, semantics
//! cross-checked against `umbra/queries/bi-N.sql`). Where GraphForge cannot run
//! the upstream text as written, [`BiQuery::rewrite`] states the change and why
//! it computes exactly the same result.
//!
//! Data model: every node has exactly one label, because the scorecard loads
//! through import sessions, which store one label per node (#952). There is no
//! `Message` supertype label, so the upstream `(m:Message)` is written `(m)`
//! with `(m:Post OR m:Comment)` in the clause's `WHERE`, which selects the same
//! nodes. The in-memory query fixture loads the same one-label model.

use crate::Operation;

/// Upstream repository whose queries these texts follow.
pub const UPSTREAM_QUERY_SOURCE: &str = "https://github.com/ldbc/ldbc_snb_bi";
/// Upstream commit the texts were checked against.
pub const UPSTREAM_QUERY_COMMIT: &str = "47dd38b40844ecdb0e42e5a610c369535304786d";

/// Kind of a bound query parameter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParameterKind {
    String,
    Int64,
    /// A UTC `datetime`, bound as `IrLiteral::ZonedDateTime`.
    DateTime,
    StringList,
}

impl ParameterKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Int64 => "int64",
            Self::DateTime => "datetime",
            Self::StringList => "string_list",
        }
    }
}

/// A named query parameter. `fixed` pins an integer the specification fixes
/// and the query text relies on (BI10's path distances).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Parameter {
    pub name: &'static str,
    pub kind: ParameterKind,
    pub fixed: Option<i64>,
}

const fn param(name: &'static str, kind: ParameterKind) -> Parameter {
    Parameter {
        name,
        kind,
        fixed: None,
    }
}

const fn fixed(name: &'static str, value: i64) -> Parameter {
    Parameter {
        name,
        kind: ParameterKind::Int64,
        fixed: Some(value),
    }
}

/// One runnable BI read.
#[derive(Clone, Copy, Debug)]
pub struct BiQuery {
    pub operation: Operation,
    pub cypher: &'static str,
    pub parameters: &'static [Parameter],
    pub columns: &'static [&'static str],
    /// Upstream reference query this text follows.
    pub upstream: &'static str,
    /// A variance from the upstream LDBC text: why GraphForge cannot run that
    /// text (citing the tracking issue where a GraphForge defect is the cause)
    /// and why the rewrite returns the same result. Always starts with
    /// `rewrite:`. `None` means the upstream text with result aliases only.
    pub rewrite: Option<&'static str>,
}

/// A read the public surface cannot express exactly.
#[derive(Clone, Copy, Debug)]
pub struct RefusedRead {
    pub operation: Operation,
    pub cause: &'static str,
    pub detail: &'static str,
}

use ParameterKind::{DateTime, Int64, String as Str, StringList};

pub const BI_QUERIES: [BiQuery; 17] = [
    BiQuery {
        operation: Operation::Bi1,
        cypher: "\
MATCH (message)
WHERE (message:Post OR message:Comment) AND message.creationDate < $datetime
WITH count(message) AS totalMessageCountInt
WITH toFloat(totalMessageCountInt) AS totalMessageCount
MATCH (message)
WHERE (message:Post OR message:Comment) AND message.creationDate < $datetime
  AND message.content IS NOT NULL
WITH totalMessageCount, message, message.creationDate.year AS year
WITH
  totalMessageCount,
  year,
  message:Comment AS isComment,
  CASE
    WHEN message.length <  40 THEN 0
    WHEN message.length <  80 THEN 1
    WHEN message.length < 160 THEN 2
    ELSE                           3
  END AS lengthCategory,
  count(message) AS messageCount,
  sum(message.length) / toFloat(count(message)) AS averageMessageLength,
  sum(message.length) AS sumMessageLength
RETURN
  year,
  isComment,
  lengthCategory,
  messageCount,
  averageMessageLength,
  sumMessageLength,
  messageCount / totalMessageCount AS percentageOfMessages
ORDER BY
  year DESC,
  isComment ASC,
  lengthCategory ASC",
        parameters: &[param("datetime", DateTime)],
        columns: &[
            "year",
            "isComment",
            "lengthCategory",
            "messageCount",
            "averageMessageLength",
            "sumMessageLength",
            "percentageOfMessages",
        ],
        upstream: "neo4j/queries/bi-1.cypher",
        rewrite: Some(
            "rewrite: LDBC text uses a `:Message` supertype label becomes `(m)` with `(m:Post OR m:Comment)` \
             in its WHERE, which selects the same nodes because import sessions assign one \
             label per node. The upstream stored creation-date component expression is preserved.",
        ),
    },
    BiQuery {
        operation: Operation::Bi2,
        cypher: "\
MATCH (tag:Tag)-[:HAS_TYPE]->(:TagClass {name: $tagClass})
OPTIONAL MATCH (message1)-[:HAS_TAG]->(tag)
  WHERE (message1:Post OR message1:Comment) AND $date <= message1.creationDate
    AND message1.creationDate < $date + duration({days: 100})
WITH tag, count(message1) AS countWindow1
OPTIONAL MATCH (message2)-[:HAS_TAG]->(tag)
  WHERE (message2:Post OR message2:Comment) AND $date + duration({days: 100}) <= message2.creationDate
    AND message2.creationDate < $date + duration({days: 200})
WITH
  tag,
  countWindow1,
  count(message2) AS countWindow2
RETURN
  tag.name AS tagName,
  countWindow1,
  countWindow2,
  abs(countWindow1 - countWindow2) AS diff
ORDER BY
  diff DESC,
  tagName ASC
LIMIT 100",
        parameters: &[param("date", DateTime), param("tagClass", Str)],
        columns: &["tagName", "countWindow1", "countWindow2", "diff"],
        upstream: "neo4j/queries/bi-2.cypher",
        rewrite: Some(
            "rewrite: LDBC text matches the `:Message` supertype label, which import sessions \
             cannot assign (one label per node). `(m:Message)` becomes `(m)` with `(m:Post OR \
             m:Comment)` in its WHERE, which selects the same nodes.",
        ),
    },
    BiQuery {
        operation: Operation::Bi3,
        cypher: "\
MATCH
  (:Country {name: $country})<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-
  (person:Person)<-[:HAS_MODERATOR]-(forum:Forum)-[:CONTAINER_OF]->
  (post:Post)<-[:REPLY_OF*0..]-(message)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->(:TagClass {name: $tagClass})
WHERE (message:Post OR message:Comment)
RETURN
  forum.id AS forumId,
  forum.title AS forumTitle,
  forum.creationDate AS forumCreationDate,
  person.id AS personId,
  count(DISTINCT message) AS messageCount
ORDER BY
  messageCount DESC,
  forumId ASC
LIMIT 20",
        parameters: &[param("tagClass", Str), param("country", Str)],
        columns: &[
            "forumId",
            "forumTitle",
            "forumCreationDate",
            "personId",
            "messageCount",
        ],
        upstream: "neo4j/queries/bi-3.cypher",
        rewrite: Some(
            "rewrite: LDBC text matches the `:Message` supertype label, which import sessions \
             cannot assign (one label per node). `(m:Message)` becomes `(m)` with `(m:Post OR \
             m:Comment)` in its WHERE, which selects the same nodes.",
        ),
    },
    BiQuery {
        operation: Operation::Bi4,
        cypher: "\
MATCH (country:Country)<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(person:Person)<-[:HAS_MEMBER]-(forum:Forum)
WHERE forum.creationDate > $date
WITH country, forum, count(person) AS numberOfMembers
WITH forum, max(numberOfMembers) AS maxNumberOfMembers
ORDER BY maxNumberOfMembers DESC, forum.id ASC
LIMIT 100
WITH collect(forum) AS topForums
MATCH (topForum:Forum)-[:HAS_MEMBER]->(person:Person)
WHERE topForum IN topForums
WITH DISTINCT topForums, person
OPTIONAL MATCH (forum:Forum)-[:CONTAINER_OF]->(:Post)<-[:REPLY_OF*0..]-(message)-[:HAS_CREATOR]->(person)
WHERE (message:Post OR message:Comment) AND forum IN topForums
WITH person, count(DISTINCT message) AS messageCount
RETURN
  person.id AS personId,
  person.firstName AS personFirstName,
  person.lastName AS personLastName,
  person.creationDate AS personCreationDate,
  messageCount
ORDER BY
  messageCount DESC,
  personId ASC
LIMIT 100",
        parameters: &[param("date", DateTime)],
        columns: &[
            "personId",
            "personFirstName",
            "personLastName",
            "personCreationDate",
            "messageCount",
        ],
        upstream: "neo4j/queries/bi-4.cypher",
        rewrite: Some(
            "rewrite: LDBC text hits #1888 D5 (CALL subquery). The top-100 forums are \
             ordered by their largest per-country member count, then id, which is the order the \
             LDBC ORDER BY + WITH DISTINCT yields and Umbra's maxNumberOfMembers. The UNION ALL \
             of members with their messages and members with 0 becomes every member of a top \
             forum with an OPTIONAL MATCH count of distinct messages in top-forum threads. \
             The `:Message` supertype label becomes `(m)` with `(m:Post OR m:Comment)` in its \
             WHERE, which selects the same nodes, because import sessions assign one label per \
             node.",
        ),
    },
    BiQuery {
        operation: Operation::Bi5,
        cypher: "\
MATCH (tag:Tag {name: $tag})<-[:HAS_TAG]-(message)-[:HAS_CREATOR]->(person:Person)
WHERE (message:Post OR message:Comment)
OPTIONAL MATCH (message)<-[likes:LIKES]-(:Person)
WITH person, message, count(likes) AS likeCount
OPTIONAL MATCH (message)<-[:REPLY_OF]-(reply:Comment)
WITH person, message, likeCount, count(reply) AS replyCount
WITH person, count(message) AS messageCount, sum(likeCount) AS likeCount, sum(replyCount) AS replyCount
RETURN
  person.id AS personId,
  replyCount,
  likeCount,
  messageCount,
  1*messageCount + 2*replyCount + 10*likeCount AS score
ORDER BY
  score DESC,
  personId ASC
LIMIT 100",
        parameters: &[param("tag", Str)],
        columns: &[
            "personId",
            "replyCount",
            "likeCount",
            "messageCount",
            "score",
        ],
        upstream: "neo4j/queries/bi-5.cypher",
        rewrite: Some(
            "rewrite: LDBC text matches the `:Message` supertype label, which import sessions \
             cannot assign (one label per node). `(m:Message)` becomes `(m)` with `(m:Post OR \
             m:Comment)` in its WHERE, which selects the same nodes.",
        ),
    },
    BiQuery {
        operation: Operation::Bi6,
        cypher: "\
MATCH (tag:Tag {name: $tag})<-[:HAS_TAG]-(message1)-[:HAS_CREATOR]->(person1:Person)
WHERE (message1:Post OR message1:Comment)
OPTIONAL MATCH (message1)<-[:LIKES]-(person2:Person)
OPTIONAL MATCH (person2)<-[:HAS_CREATOR]-(message2)<-[like:LIKES]-(person3:Person)
WHERE (message2:Post OR message2:Comment)
RETURN
  person1.id AS person1Id,
  count(DISTINCT like) AS authorityScore
ORDER BY
  authorityScore DESC,
  person1Id ASC
LIMIT 100",
        parameters: &[param("tag", Str)],
        columns: &["person1Id", "authorityScore"],
        upstream: "neo4j/queries/bi-6.cypher",
        rewrite: Some(
            "rewrite: LDBC text matches the `:Message` supertype label, which import sessions \
             cannot assign (one label per node). `(m:Message)` becomes `(m)` with `(m:Post OR \
             m:Comment)` in its WHERE, which selects the same nodes.",
        ),
    },
    BiQuery {
        operation: Operation::Bi7,
        cypher: "\
MATCH
  (tag:Tag {name: $tag})<-[:HAS_TAG]-(message),
  (message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_TAG]->(relatedTag:Tag)
WHERE (message:Post OR message:Comment)
  AND NOT (comment)-[:HAS_TAG]->(tag)
RETURN
  relatedTag.name AS relatedTagName,
  count(DISTINCT comment) AS count
ORDER BY
  count DESC,
  relatedTagName ASC
LIMIT 100",
        parameters: &[param("tag", Str)],
        columns: &["relatedTagName", "count"],
        upstream: "neo4j/queries/bi-7.cypher",
        rewrite: Some(
            "rewrite: LDBC text matches the `:Message` supertype label, which import sessions \
             cannot assign (one label per node). `(m:Message)` becomes `(m)` with `(m:Post OR \
             m:Comment)` in its WHERE, which selects the same nodes.",
        ),
    },
    BiQuery {
        operation: Operation::Bi8,
        cypher: "\
MATCH (tag:Tag {name: $tag})
OPTIONAL MATCH (tag)<-[interest:HAS_INTEREST]-(person:Person)
WITH tag, collect(person) AS interestedPersons
OPTIONAL MATCH (tag)<-[:HAS_TAG]-(message)-[:HAS_CREATOR]->(person:Person)
         WHERE (message:Post OR message:Comment)
           AND $startDate < message.creationDate
           AND message.creationDate < $endDate
WITH tag, interestedPersons, interestedPersons + collect(person) AS persons
UNWIND persons AS person
WITH DISTINCT tag, person
WITH
  tag,
  person,
  100 * size([(tag)<-[interest:HAS_INTEREST]-(person) | interest]) + size([(tag)<-[:HAS_TAG]-(message)-[:HAS_CREATOR]->(person) WHERE (message:Post OR message:Comment) AND $startDate < message.creationDate AND message.creationDate < $endDate | message])
  AS score
OPTIONAL MATCH (person)-[:KNOWS]-(friend)
WITH
  person,
  score,
  100 * size([(tag)<-[interest:HAS_INTEREST]-(friend) | interest]) + size([(tag)<-[:HAS_TAG]-(message)-[:HAS_CREATOR]->(friend) WHERE (message:Post OR message:Comment) AND $startDate < message.creationDate AND message.creationDate < $endDate | message])
  AS friendScore
RETURN
  person.id AS personId,
  score,
  sum(friendScore) AS friendsScore
ORDER BY
  score + friendsScore DESC,
  personId ASC
LIMIT 100",
        parameters: &[
            param("tag", Str),
            param("startDate", DateTime),
            param("endDate", DateTime),
        ],
        columns: &["personId", "score", "friendsScore"],
        upstream: "neo4j/queries/bi-8.cypher",
        rewrite: Some(
            "rewrite: LDBC text uses a `:Message` supertype label becomes `(m)` with `(m:Post OR m:Comment)` \
             in its WHERE, which selects the same nodes because import sessions assign one \
             label per node. Upstream collect/concatenate/UNWIND node values are preserved.",
        ),
    },
    BiQuery {
        operation: Operation::Bi9,
        cypher: "\
MATCH (person:Person)<-[:HAS_CREATOR]-(post:Post)<-[:REPLY_OF*0..]-(reply)
WHERE  (reply:Post OR reply:Comment)
  AND  post.creationDate >= $startDate
  AND  post.creationDate <= $endDate
  AND reply.creationDate >= $startDate
  AND reply.creationDate <= $endDate
RETURN
  person.id AS personId,
  person.firstName AS personFirstName,
  person.lastName AS personLastName,
  count(DISTINCT post) AS threadCount,
  count(DISTINCT reply) AS messageCount
ORDER BY
  messageCount DESC,
  personId ASC
LIMIT 100",
        parameters: &[param("startDate", DateTime), param("endDate", DateTime)],
        columns: &[
            "personId",
            "personFirstName",
            "personLastName",
            "threadCount",
            "messageCount",
        ],
        upstream: "neo4j/queries/bi-9.cypher",
        rewrite: Some(
            "rewrite: LDBC text matches the `:Message` supertype label, which import sessions \
             cannot assign (one label per node). `(m:Message)` becomes `(m)` with `(m:Post OR \
             m:Comment)` in its WHERE, which selects the same nodes.",
        ),
    },
    BiQuery {
        operation: Operation::Bi10,
        cypher: "\
MATCH path = (startPerson:Person {id: $personId})-[:KNOWS*1..4]-(expertCandidatePerson:Person)
WHERE expertCandidatePerson <> startPerson
WITH expertCandidatePerson, min(length(path)) AS distance
WHERE $minPathDistance <= distance AND distance <= $maxPathDistance
MATCH
  (expertCandidatePerson)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(:Country {name: $country}),
  (expertCandidatePerson)<-[:HAS_CREATOR]-(message)-[:HAS_TAG]->(:Tag)-[:HAS_TYPE]->
  (:TagClass {name: $tagClass})
WHERE (message:Post OR message:Comment)
MATCH
  (message)-[:HAS_TAG]->(tag:Tag)
RETURN
  expertCandidatePerson.id AS personId,
  tag.name AS tagName,
  count(DISTINCT message) AS messageCount
ORDER BY
  messageCount DESC,
  tagName ASC,
  personId ASC
LIMIT 100",
        parameters: &[
            param("personId", Int64),
            param("country", Str),
            param("tagClass", Str),
            fixed("minPathDistance", 3),
            fixed("maxPathDistance", 4),
        ],
        columns: &["personId", "tagName", "messageCount"],
        upstream: "neo4j/queries/bi-10.cypher",
        rewrite: Some(
            "rewrite: LDBC text calls the APOC procedure `apoc.path.subgraphNodes`, which \
             GraphForge does not provide. APOC keeps persons whose shortest KNOWS distance lies \
             in [minPathDistance, maxPathDistance]; here the distance is the minimum length over \
             KNOWS trails of length 1..4 from the start person, which equals the shortest-path \
             distance for every person within 4 hops (a shortest walk is a trail). The start \
             person is excluded, as APOC's minLevel 1 excludes it. The specification fixes the \
             distances at 3 and 4 (umbra/queries/bi-10.sql), the pattern bound 4 relies on that, \
             and the runner refuses any other binding. The `:Message` supertype label becomes `(m)` with `(m:Post OR m:Comment)` in its \
             WHERE, which selects the same nodes, because import sessions assign one label per \
             node.",
        ),
    },
    BiQuery {
        operation: Operation::Bi11,
        cypher: "\
MATCH (a:Person)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country:Country {name: $country}),
      (a)-[k1:KNOWS]-(b:Person)
WHERE a.id < b.id
  AND $startDate <= k1.creationDate AND k1.creationDate <= $endDate
WITH DISTINCT country, a, b
MATCH (b)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
WITH DISTINCT country, a, b
MATCH (b)-[k2:KNOWS]-(c:Person),
      (c)-[:IS_LOCATED_IN]->(:City)-[:IS_PART_OF]->(country)
WHERE b.id < c.id
  AND $startDate <= k2.creationDate AND k2.creationDate <= $endDate
WITH DISTINCT a, b, c
MATCH (c)-[k3:KNOWS]-(a)
WHERE $startDate <= k3.creationDate AND k3.creationDate <= $endDate
WITH DISTINCT a, b, c
RETURN count(*) AS count",
        parameters: &[
            param("country", Str),
            param("startDate", DateTime),
            param("endDate", DateTime),
        ],
        columns: &["count"],
        upstream: "neo4j/queries/bi-11.cypher",
        rewrite: None,
    },
    BiQuery {
        operation: Operation::Bi12,
        cypher: "\
MATCH (person:Person)
OPTIONAL MATCH (person)<-[:HAS_CREATOR]-(message)-[:REPLY_OF*0..]->(post:Post)
WHERE (message:Post OR message:Comment)
  AND message.content IS NOT NULL
  AND message.length < $lengthThreshold
  AND message.creationDate > $startDate
  AND post.language IN $languages
WITH
  person,
  count(message) AS messageCount
RETURN
  messageCount,
  count(person) AS personCount
ORDER BY
  personCount DESC,
  messageCount DESC",
        parameters: &[
            param("startDate", DateTime),
            param("lengthThreshold", Int64),
            param("languages", StringList),
        ],
        columns: &["messageCount", "personCount"],
        upstream: "neo4j/queries/bi-12.cypher",
        rewrite: Some(
            "rewrite: LDBC text matches the `:Message` supertype label, which import sessions \
             cannot assign (one label per node). `(m:Message)` becomes `(m)` with `(m:Post OR \
             m:Comment)` in its WHERE, which selects the same nodes.",
        ),
    },
    BiQuery {
        operation: Operation::Bi13,
        cypher: "\
MATCH (country:Country {name: $country})<-[:IS_PART_OF]-(:City)<-[:IS_LOCATED_IN]-(zombie:Person)
WHERE zombie.creationDate < $endDate
WITH country, zombie
OPTIONAL MATCH (zombie)<-[:HAS_CREATOR]-(message)
WHERE (message:Post OR message:Comment) AND message.creationDate < $endDate
WITH
  country,
  zombie,
  count(message) AS messageCount
WITH
  country,
  zombie,
  12 * ($endDate.year  - zombie.creationDate.year )
     + ($endDate.month - zombie.creationDate.month)
     + 1 AS months,
  messageCount
WHERE messageCount / months < 1
WITH
  country,
  collect(zombie) AS zombies
UNWIND zombies AS zombie
OPTIONAL MATCH
  (zombie)<-[:HAS_CREATOR]-(message)<-[:LIKES]-(likerZombie:Person)
WHERE (message:Post OR message:Comment) AND likerZombie IN zombies
WITH
  zombie,
  count(likerZombie) AS zombieLikeCount
OPTIONAL MATCH
  (zombie)<-[:HAS_CREATOR]-(message)<-[:LIKES]-(likerPerson:Person)
WHERE (message:Post OR message:Comment) AND likerPerson.creationDate < $endDate
WITH
  zombie,
  zombieLikeCount,
  count(likerPerson) AS totalLikeCount
RETURN
  zombie.id AS zombieId,
  zombieLikeCount,
  totalLikeCount,
  CASE totalLikeCount
    WHEN 0 THEN 0.0
    ELSE zombieLikeCount / toFloat(totalLikeCount)
  END AS zombieScore
ORDER BY
  zombieScore DESC,
  zombieId ASC
LIMIT 100",
        parameters: &[param("country", Str), param("endDate", DateTime)],
        columns: &["zombieId", "zombieLikeCount", "totalLikeCount", "zombieScore"],
        upstream: "neo4j/queries/bi-13.cypher",
        rewrite: Some(
            "rewrite: LDBC text uses a `:Message` supertype label becomes `(m)` with `(m:Post OR m:Comment)` \
             in its WHERE, which selects the same nodes because import sessions assign one \
             label per node. The upstream stored creation-date component arithmetic is preserved.",
        ),
    },
    BiQuery {
        operation: Operation::Bi14,
        cypher: "\
MATCH
  (country1:Country {name: $country1})<-[:IS_PART_OF]-(city1:City)<-[:IS_LOCATED_IN]-(person1:Person),
  (country2:Country {name: $country2})<-[:IS_PART_OF]-(city2:City)<-[:IS_LOCATED_IN]-(person2:Person),
  (person1)-[:KNOWS]-(person2)
WITH person1, person2, city1, 0 AS score
OPTIONAL MATCH (person1)<-[:HAS_CREATOR]-(c:Comment)-[:REPLY_OF]->(parent)-[:HAS_CREATOR]->(person2)
WHERE (parent:Post OR parent:Comment)
WITH DISTINCT person1, person2, city1, score + (CASE WHEN c IS NULL THEN 0 ELSE  4 END) AS score
OPTIONAL MATCH (person1)<-[:HAS_CREATOR]-(m)<-[:REPLY_OF]-(:Comment)-[:HAS_CREATOR]->(person2)
WHERE (m:Post OR m:Comment)
WITH DISTINCT person1, person2, city1, score + (CASE WHEN m IS NULL THEN 0 ELSE  1 END) AS score
OPTIONAL MATCH (person1)-[:LIKES]->(m)-[:HAS_CREATOR]->(person2)
WHERE (m:Post OR m:Comment)
WITH DISTINCT person1, person2, city1, score + (CASE WHEN m IS NULL THEN 0 ELSE 10 END) AS score
OPTIONAL MATCH (person1)<-[:HAS_CREATOR]-(m)<-[:LIKES]-(person2)
WHERE (m:Post OR m:Comment)
WITH DISTINCT person1, person2, city1, score + (CASE WHEN m IS NULL THEN 0 ELSE  1 END) AS score
WITH city1, max(score) AS topScore, collect({score: score, person1Id: person1.id, person2Id: person2.id}) AS pairs
UNWIND pairs AS pair
WITH city1, topScore, pair
WHERE pair.score = topScore
WITH city1, topScore, min(pair.person1Id) AS topPerson1Id, collect(pair) AS best
UNWIND best AS pair
WITH city1, topScore, topPerson1Id, pair
WHERE pair.person1Id = topPerson1Id
WITH city1, topScore, topPerson1Id, min(pair.person2Id) AS topPerson2Id
RETURN
  topPerson1Id AS person1Id,
  topPerson2Id AS person2Id,
  city1.name AS city1Name,
  topScore AS score
ORDER BY
  score DESC,
  person1Id ASC,
  person2Id ASC
LIMIT 100",
        parameters: &[param("country1", Str), param("country2", Str)],
        columns: &["person1Id", "person2Id", "city1Name", "score"],
        upstream: "neo4j/queries/bi-14.cypher",
        rewrite: Some(
            "rewrite: LDBC text picks each city's top pair as `collect(...)[0]` after an ORDER \
             BY, which depends on aggregation keeping input order; openCypher does not guarantee \
             that. The pair is chosen explicitly as the highest score, then the lowest person1 \
             id, then the lowest person2 id, which is the pair the LDBC ordering puts first. The `:Message` supertype label becomes `(m)` with `(m:Post OR m:Comment)` in its \
             WHERE, which selects the same nodes, because import sessions assign one label per \
             node.",
        ),
    },
    BiQuery {
        operation: Operation::Bi16,
        cypher: "\
MATCH (person1:Person)<-[:HAS_CREATOR]-(message1)-[:HAS_TAG]->(tag:Tag {name: $tagA})
WHERE (message1:Post OR message1:Comment) AND date(message1.creationDate) = date($dateA)
OPTIONAL MATCH (person1)-[:KNOWS]-(person2:Person)<-[:HAS_CREATOR]-(message2)-[:HAS_TAG]->(tag)
WHERE (message2:Post OR message2:Comment) AND date(message2.creationDate) = date($dateA)
WITH person1, count(DISTINCT message1) AS cm, count(DISTINCT person2) AS cp2
WHERE cp2 <= $maxKnowsLimit
WITH person1, cm AS messageCountA
MATCH (person1)<-[:HAS_CREATOR]-(message1)-[:HAS_TAG]->(tag:Tag {name: $tagB})
WHERE (message1:Post OR message1:Comment) AND date(message1.creationDate) = date($dateB)
OPTIONAL MATCH (person1)-[:KNOWS]-(person2:Person)<-[:HAS_CREATOR]-(message2)-[:HAS_TAG]->(tag)
WHERE (message2:Post OR message2:Comment) AND date(message2.creationDate) = date($dateB)
WITH person1, messageCountA, count(DISTINCT message1) AS cm, count(DISTINCT person2) AS cp2
WHERE cp2 <= $maxKnowsLimit
RETURN
  person1.id AS personId,
  messageCountA,
  cm AS messageCountB
ORDER BY messageCountA + messageCountB DESC, personId ASC
LIMIT 20",
        parameters: &[
            param("tagA", Str),
            param("dateA", DateTime),
            param("tagB", Str),
            param("dateB", DateTime),
            param("maxKnowsLimit", Int64),
        ],
        columns: &["personId", "messageCountA", "messageCountB"],
        upstream: "neo4j/queries/bi-16.cypher",
        rewrite: Some(
            "rewrite: LDBC text hits #1888 D5 (CALL subquery). LDBC runs the same per-person \
             subquery for (tagA, dateA) and (tagB, dateB) and keeps persons that pass both; here \
             the A pass runs over all persons and the B pass runs for each person that passed A. \
             A person's B counts depend only on that person, so the surviving persons and their \
             counts are the same. The `:Message` supertype label becomes `(m)` with `(m:Post OR m:Comment)` in its \
             WHERE, which selects the same nodes, because import sessions assign one label per \
             node.",
        ),
    },
    BiQuery {
        operation: Operation::Bi17,
        cypher: "\
MATCH
  (tag:Tag {name: $tag}),
  (person1:Person)<-[:HAS_CREATOR]-(message1)-[:REPLY_OF*0..]->(post1:Post)<-[:CONTAINER_OF]-(forum1:Forum),
  (message1)-[:HAS_TAG]->(tag),
  (forum1)<-[:HAS_MEMBER]->(person2:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:HAS_TAG]->(tag),
  (forum1)<-[:HAS_MEMBER]->(person3:Person)<-[:HAS_CREATOR]-(message2),
  (comment)-[:REPLY_OF]->(message2)-[:REPLY_OF*0..]->(post2:Post)<-[:CONTAINER_OF]-(forum2:Forum)
WHERE (message1:Post OR message1:Comment) AND (message2:Post OR message2:Comment)
MATCH (comment)-[:HAS_TAG]->(tag)
MATCH (message2)-[:HAS_TAG]->(tag)
WITH
  person1,
  message2,
  forum1,
  forum2,
  message1.creationDate AS message1CreationDate,
  message2.creationDate AS message2CreationDate
WHERE forum1 <> forum2
  AND message2CreationDate.epochMillis > message1CreationDate.epochMillis + $delta * 3600000
  AND NOT (forum2)-[:HAS_MEMBER]->(person1)
RETURN person1.id AS person1Id, count(DISTINCT message2) AS messageCount
ORDER BY messageCount DESC, person1Id ASC
LIMIT 10",
        parameters: &[param("tag", Str), param("delta", Int64)],
        columns: &["person1Id", "messageCount"],
        upstream: "neo4j/queries/bi-17.cypher",
        rewrite: Some(
            "rewrite: LDBC text hits #1888 D4 (`duration({hours: $delta})` with a non-literal \
             argument fails). The delta becomes a comparison of epoch milliseconds with \
             `$delta * 3600000` added, which is exact for whole hours on a UTC instant. The \
             final WHERE moves onto a WITH carrying the compared values. The `:Message` supertype label becomes `(m)` with `(m:Post OR m:Comment)` in its \
             WHERE, which selects the same nodes, because import sessions assign one label per \
             node.",
        ),
    },
    BiQuery {
        operation: Operation::Bi18,
        cypher: "\
MATCH (tag:Tag {name: $tag})<-[:HAS_INTEREST]-(person1:Person)-[:KNOWS]-(mutualFriend:Person)-[:KNOWS]-(person2:Person)-[:HAS_INTEREST]->(tag)
WHERE person1 <> person2
  AND NOT (person1)-[:KNOWS]-(person2)
RETURN person1.id AS person1Id, person2.id AS person2Id, count(DISTINCT mutualFriend) AS mutualFriendCount
ORDER BY mutualFriendCount DESC, person1Id ASC, person2Id ASC
LIMIT 20",
        parameters: &[param("tag", Str)],
        columns: &["person1Id", "person2Id", "mutualFriendCount"],
        upstream: "neo4j/queries/bi-18.cypher",
        rewrite: None,
    },
];

pub const WEIGHTED_SHORTEST_PATH_NOT_EXPOSED: &str = "weighted_shortest_path_not_exposed";

pub const REFUSED_READS: [RefusedRead; 3] = [
    RefusedRead {
        operation: Operation::Bi15,
        cause: WEIGHTED_SHORTEST_PATH_NOT_EXPOSED,
        detail: "BI15 computes the minimum-weight trusted connection path between two persons where each \
                 KNOWS edge weight is a dynamically computed interaction cost; the public surface exposes \
                 unweighted single-path analyst verbs and pattern matching but not weighted shortest-path \
                 search over a computed edge-weight function",
    },
    RefusedRead {
        operation: Operation::Bi19,
        cause: WEIGHTED_SHORTEST_PATH_NOT_EXPOSED,
        detail: "BI19 finds the minimum-cost interaction path between persons located in two cities, where \
                 each KNOWS edge cost is derived from the reciprocal of the reply/comment interaction count; \
                 weighted shortest-path search over a computed edge-weight function is not on the public surface",
    },
    RefusedRead {
        operation: Operation::Bi20,
        cause: WEIGHTED_SHORTEST_PATH_NOT_EXPOSED,
        detail: "BI20 (recruitment) computes the minimum-weight path from a person to a company's employees \
                 over the KNOWS graph with per-person derived edge weights; the public surface exposes \
                 unweighted path verbs only, not weighted shortest-path search over a computed weight function",
    },
];

/// The runnable definition of `operation`, if it has one.
pub fn bi_query(operation: Operation) -> Option<&'static BiQuery> {
    BI_QUERIES.iter().find(|query| query.operation == operation)
}

/// The typed refusal of `operation`, if it is a refused read.
pub fn refused_read(operation: Operation) -> Option<&'static RefusedRead> {
    REFUSED_READS
        .iter()
        .find(|refusal| refusal.operation == operation)
}
