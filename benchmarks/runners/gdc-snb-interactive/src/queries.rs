//! Runnable SNB Interactive v1 read queries, exposed as data.
//!
//! Each [`QueryDefinition`] names its operation, the public GraphForge surface
//! that executes it, the parameters it binds and the result columns it
//! returns. A driver iterates [`query_definitions`] and executes each entry
//! with [`crate::live_queries::execute_query`] against any open `GraphForge`
//! (in-memory or durable).
//!
//! Semantics follow the pinned LDBC SNB Interactive v1 Cypher reference
//! implementation (`ldbc_snb_interactive_v1_impls` at
//! `f9c394a92cd55e535893f6c9907b141d6533c817`, `cypher/queries/`), including
//! its ordering, tie-breakers, `LIMIT` and parameter names. Where the reference
//! text uses a construct GraphForge does not evaluate the same way, the query
//! below is rewritten to an exactly equivalent form; each rewrite is listed in
//! the definition's `notes`. Nothing is approximated: an operation without an
//! exact formulation is refused in [`crate::map_operation`] instead.
//!
//! Data model (#952 decision 2026-10-07: Interactive follows the v1 reference
//! data model): dates and datetimes are epoch milliseconds (UTC) in `Int64`
//! properties, `Person.email` and `Person.speaks` are string lists, and `id` is
//! unique within each entity type. Every node has exactly one label, because
//! import sessions store one label per node, so there is no `Message`
//! supertype label: the reference's `(m:Message)` is written
//! `(m) WHERE (m:Post OR m:Comment)`, which selects the same nodes.

use crate::Operation;

/// Bound parameter type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParameterType {
    Int64,
    Utf8,
}

impl ParameterType {
    pub fn name(self) -> &'static str {
        match self {
            Self::Int64 => "int64",
            Self::Utf8 => "utf8",
        }
    }
}

/// One named query parameter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryParameter {
    pub name: &'static str,
    pub data_type: ParameterType,
}

/// The public GraphForge surface that executes a query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryInterface {
    /// Cypher text run by `GraphForge::execute_with_params`.
    Cypher(&'static str),
    /// Unweighted shortest-path length run by `GraphForge::paths` with
    /// `by=bfs`, undirected, over one relationship type. Both endpoints are
    /// selected by `label`/`id_property` from the two parameters; no row from
    /// the verb means no path (`-1`), and equal endpoints have length `0`.
    BfsPathLength {
        label: &'static str,
        id_property: &'static str,
        relationship_type: &'static str,
        source_parameter: &'static str,
        target_parameter: &'static str,
    },
}

/// A runnable SNB Interactive read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryDefinition {
    pub operation: Operation,
    pub interface: QueryInterface,
    pub parameters: &'static [QueryParameter],
    /// Result column names, in order.
    pub columns: &'static [&'static str],
    /// List-valued columns whose element order the specification leaves
    /// unspecified (sets); validation compares them as sorted lists.
    pub unordered_list_columns: &'static [&'static str],
    /// The specification's row limit, if any.
    pub limit: Option<usize>,
    /// Differences from the reference query text, and why they are exact.
    pub notes: &'static str,
}

impl QueryDefinition {
    /// Cypher text, or `None` for the analyst-verb operation.
    pub fn cypher(&self) -> Option<&'static str> {
        match self.interface {
            QueryInterface::Cypher(text) => Some(text),
            QueryInterface::BfsPathLength { .. } => None,
        }
    }

    /// Parameter names in declaration order.
    pub fn parameter_names(&self) -> Vec<&'static str> {
        self.parameters
            .iter()
            .map(|parameter| parameter.name)
            .collect()
    }
}

const fn int(name: &'static str) -> QueryParameter {
    QueryParameter {
        name,
        data_type: ParameterType::Int64,
    }
}

const fn text(name: &'static str) -> QueryParameter {
    QueryParameter {
        name,
        data_type: ParameterType::Utf8,
    }
}

// Shared rewrite rationale, referenced from per-query notes.
//
// * GraphForge does not parse `shortestPath`; IC1 takes the minimum length over
//   the bounded variable-length match instead. A shortest path is a simple path
//   and therefore one of the matched trails, so the minimum is the same value.
// * GraphForge's `IN` does not match node values inside a list, so node-list
//   membership is expressed on the entity's unique `id`.
// * `CASE x WHEN null` compares with `=` in openCypher and never matches null;
//   the reference's intent (no university, no KNOWS edge) is written `IS NULL`.
// * `datetime({epochMillis: ...})` returns the epoch in GraphForge, so IC10
//   derives the UTC calendar month and day from the epoch milliseconds with
//   exact integer arithmetic (Hinnant's civil-from-days algorithm).
// * `:Message` becomes `(m:Post OR m:Comment)`; see the module data model.
// * Pattern predicates are valid only in `WHERE`; a pattern used as a value is
//   written as a pattern comprehension or a counted `OPTIONAL MATCH`.
// * GraphForge does not apply a `WHERE` that holds an `EXISTS` predicate, and
//   fails to plan one that tests a list membership, when it follows an
//   `OPTIONAL MATCH`; those filters move into the pattern or a conditional sum.

const IC1: &str = "\
MATCH path = (p:Person {id: $personId})-[:KNOWS*1..3]-(friend:Person {firstName: $firstName})
WHERE NOT p = friend
WITH friend, min(length(path)) AS distance
ORDER BY distance ASC, friend.lastName ASC, toInteger(friend.id) ASC
LIMIT 20
MATCH (friend)-[:IS_LOCATED_IN]->(friendCity:City)
OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:University)-[:IS_LOCATED_IN]->(uniCity:City)
WITH friend, collect(
    CASE WHEN uni IS NULL THEN null
    ELSE {name: uni.name, classYear: studyAt.classYear, city: uniCity.name} END) AS unis,
    friendCity, distance
OPTIONAL MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[:IS_LOCATED_IN]->(companyCountry:Country)
WITH friend, collect(
    CASE WHEN company IS NULL THEN null
    ELSE {name: company.name, workFrom: workAt.workFrom, country: companyCountry.name} END) AS companies,
    unis, friendCity, distance
RETURN
    friend.id AS friendId,
    friend.lastName AS friendLastName,
    distance AS distanceFromPerson,
    friend.birthday AS friendBirthday,
    friend.creationDate AS friendCreationDate,
    friend.gender AS friendGender,
    friend.browserUsed AS friendBrowserUsed,
    friend.locationIP AS friendLocationIp,
    friend.email AS friendEmails,
    friend.speaks AS friendLanguages,
    friendCity.name AS friendCityName,
    unis AS friendUniversities,
    companies AS friendCompanies
ORDER BY distanceFromPerson ASC, friendLastName ASC, toInteger(friendId) ASC
LIMIT 20";

const IC2: &str = "\
MATCH (:Person {id: $personId})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(message)
WHERE (message:Post OR message:Comment) AND message.creationDate <= $maxDate
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    message.id AS postOrCommentId,
    coalesce(message.content, message.imageFile) AS postOrCommentContent,
    message.creationDate AS postOrCommentCreationDate
ORDER BY postOrCommentCreationDate DESC, toInteger(postOrCommentId) ASC
LIMIT 20";

const IC3: &str = "\
MATCH (countryX:Country {name: $countryXName}),
      (countryY:Country {name: $countryYName}),
      (person:Person {id: $personId})
WITH person, countryX, countryY
LIMIT 1
MATCH (city:City)-[:IS_PART_OF]->(country:Country)
WHERE country.id IN [countryX.id, countryY.id]
WITH person, countryX, countryY, collect(city.id) AS cities
MATCH (person)-[:KNOWS*1..2]-(friend)-[:IS_LOCATED_IN]->(city)
WHERE NOT person = friend AND NOT city.id IN cities
WITH DISTINCT friend, countryX, countryY
MATCH (friend)<-[:HAS_CREATOR]-(message),
      (message)-[:IS_LOCATED_IN]->(country)
WHERE $endDate > message.creationDate >= $startDate AND
      country.id IN [countryX.id, countryY.id]
WITH friend,
     CASE WHEN country.id = countryX.id THEN 1 ELSE 0 END AS messageX,
     CASE WHEN country.id = countryY.id THEN 1 ELSE 0 END AS messageY
WITH friend, sum(messageX) AS xCount, sum(messageY) AS yCount
WHERE xCount > 0 AND yCount > 0
RETURN friend.id AS friendId,
       friend.firstName AS friendFirstName,
       friend.lastName AS friendLastName,
       xCount,
       yCount,
       xCount + yCount AS xyCount
ORDER BY xyCount DESC, friendId ASC
LIMIT 20";

const IC4: &str = "\
MATCH (person:Person {id: $personId})-[:KNOWS]-(friend:Person),
      (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag)
WITH DISTINCT tag, post
WITH tag,
     CASE
       WHEN $endDate > post.creationDate >= $startDate THEN 1
       ELSE 0
     END AS valid,
     CASE
       WHEN $startDate > post.creationDate THEN 1
       ELSE 0
     END AS inValid
WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
WHERE postCount > 0 AND inValidPostCount = 0
RETURN tag.name AS tagName, postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10";

const IC5: &str = "\
MATCH (person:Person {id: $personId})-[:KNOWS*1..2]-(friend)
WHERE NOT person = friend
WITH DISTINCT friend
MATCH (friend)<-[membership:HAS_MEMBER]-(forum)
WHERE membership.joinDate > $minDate
WITH forum, collect(friend.id) AS friendIds
OPTIONAL MATCH (forum)-[:CONTAINER_OF]->(post)-[:HAS_CREATOR]->(author)
WITH forum,
     sum(CASE WHEN post IS NOT NULL AND author.id IN friendIds THEN 1 ELSE 0 END) AS postCount
RETURN forum.title AS forumName, postCount
ORDER BY postCount DESC, forum.id ASC
LIMIT 20";

const IC6: &str = "\
MATCH (knownTag:Tag {name: $tagName})
WITH knownTag.id AS knownTagId
MATCH (person:Person {id: $personId})-[:KNOWS*1..2]-(friend)
WHERE NOT person = friend
WITH knownTagId, collect(DISTINCT friend) AS friends
UNWIND friends AS f
    MATCH (f)<-[:HAS_CREATOR]-(post:Post),
          (post)-[:HAS_TAG]->(t:Tag {id: knownTagId}),
          (post)-[:HAS_TAG]->(tag:Tag)
    WHERE NOT t = tag
    WITH tag.name AS tagName, count(post) AS postCount
RETURN tagName, postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10";

const IC7: &str = "\
MATCH (person:Person {id: $personId})<-[:HAS_CREATOR]-(message)<-[like:LIKES]-(liker:Person)
WHERE message:Post OR message:Comment
WITH person, liker, max(like.creationDate) AS likeTime
MATCH (person)<-[:HAS_CREATOR]-(message)<-[like:LIKES]-(liker)
WHERE (message:Post OR message:Comment) AND like.creationDate = likeTime
WITH person, liker, likeTime, min(message.id) AS latestMessageId
MATCH (person)<-[:HAS_CREATOR]-(message)
WHERE (message:Post OR message:Comment) AND message.id = latestMessageId
RETURN
    liker.id AS personId,
    liker.firstName AS personFirstName,
    liker.lastName AS personLastName,
    likeTime AS likeCreationDate,
    message.id AS commentOrPostId,
    coalesce(message.content, message.imageFile) AS commentOrPostContent,
    toInteger(floor(toFloat(likeTime - message.creationDate) / 1000.0) / 60.0) AS minutesLatency,
    size([(liker)-[:KNOWS]-(person) | 1]) = 0 AS isNew
ORDER BY likeCreationDate DESC, toInteger(personId) ASC
LIMIT 20";

const IC8: &str = "\
MATCH (start:Person {id: $personId})<-[:HAS_CREATOR]-(message)<-[:REPLY_OF]-(comment:Comment)-[:HAS_CREATOR]->(person:Person)
WHERE message:Post OR message:Comment
RETURN
    person.id AS personId,
    person.firstName AS personFirstName,
    person.lastName AS personLastName,
    comment.creationDate AS commentCreationDate,
    comment.id AS commentId,
    comment.content AS commentContent
ORDER BY commentCreationDate DESC, commentId ASC
LIMIT 20";

const IC9: &str = "\
MATCH (root:Person {id: $personId})-[:KNOWS*1..2]-(friend:Person)
WHERE NOT friend = root
WITH collect(DISTINCT friend) AS friends
UNWIND friends AS friend
    MATCH (friend)<-[:HAS_CREATOR]-(message)
    WHERE (message:Post OR message:Comment) AND message.creationDate < $maxDate
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    message.id AS commentOrPostId,
    coalesce(message.content, message.imageFile) AS commentOrPostContent,
    message.creationDate AS commentOrPostCreationDate
ORDER BY commentOrPostCreationDate DESC, commentOrPostId ASC
LIMIT 20";

const IC10: &str = "\
MATCH (person:Person {id: $personId})-[:KNOWS*2..2]-(friend),
      (friend)-[:IS_LOCATED_IN]->(city:City)
WHERE NOT friend = person AND
      NOT (friend)-[:KNOWS]-(person)
WITH DISTINCT person, friend, city
WITH person, friend, city,
     CASE WHEN friend.birthday >= 0 THEN friend.birthday / 86400000
     ELSE (friend.birthday - 86399999) / 86400000 END + 719468 AS z
WITH person, friend, city, z,
     CASE WHEN z >= 0 THEN z ELSE z - 146096 END / 146097 AS era
WITH person, friend, city, z - era * 146097 AS doe
WITH person, friend, city, doe,
     (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365 AS yoe
WITH person, friend, city, doe - (365 * yoe + yoe / 4 - yoe / 100) AS doy
WITH person, friend, city, doy, (5 * doy + 2) / 153 AS mp
WITH person, friend, city,
     doy - (153 * mp + 2) / 5 + 1 AS birthdayDay,
     CASE WHEN mp < 10 THEN mp + 3 ELSE mp - 9 END AS birthdayMonth
WHERE (birthdayMonth = $month AND birthdayDay >= 21) OR
      (birthdayMonth = ($month % 12) + 1 AND birthdayDay < 22)
OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)
WITH person, friend, city, count(post) AS postCount
OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(:Tag)<-[:HAS_INTEREST]-(person)
WITH friend, city, postCount, count(DISTINCT post.id) AS commonPostCount
RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       commonPostCount - (postCount - commonPostCount) AS commonInterestScore,
       friend.gender AS personGender,
       city.name AS personCityName
ORDER BY commonInterestScore DESC, personId ASC
LIMIT 10";

const IC11: &str = "\
MATCH (person:Person {id: $personId})-[:KNOWS*1..2]-(friend:Person)
WHERE NOT(person = friend)
WITH DISTINCT friend
MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[:IS_LOCATED_IN]->(:Country {name: $countryName})
WHERE workAt.workFrom < $workFromYear
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    company.name AS organizationName,
    workAt.workFrom AS organizationWorkFromYear
ORDER BY organizationWorkFromYear ASC, toInteger(personId) ASC, organizationName DESC
LIMIT 10";

const IC12: &str = "\
MATCH (tag:Tag)-[:HAS_TYPE]->(:TagClass)-[:IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)
WHERE tag.name = $tagClassName OR baseTagClass.name = $tagClassName
WITH collect(tag.id) AS tags
MATCH (:Person {id: $personId})-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(comment:Comment)-[:REPLY_OF]->(:Post)-[:HAS_TAG]->(tag:Tag)
WHERE tag.id IN tags
RETURN
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName,
    collect(DISTINCT tag.name) AS tagNames,
    count(DISTINCT comment) AS replyCount
ORDER BY replyCount DESC, toInteger(personId) ASC
LIMIT 20";

const IS1: &str = "\
MATCH (n:Person {id: $personId})-[:IS_LOCATED_IN]->(p:City)
RETURN
    n.firstName AS firstName,
    n.lastName AS lastName,
    n.birthday AS birthday,
    n.locationIP AS locationIP,
    n.browserUsed AS browserUsed,
    p.id AS cityId,
    n.gender AS gender,
    n.creationDate AS creationDate";

const IS2: &str = "\
MATCH (:Person {id: $personId})<-[:HAS_CREATOR]-(message)
WITH
 message,
 message.id AS messageId,
 message.creationDate AS messageCreationDate
ORDER BY messageCreationDate DESC, messageId ASC
LIMIT 10
MATCH (message)-[:REPLY_OF*0..]->(post:Post),
      (post)-[:HAS_CREATOR]->(person)
RETURN
 messageId,
 coalesce(message.imageFile, message.content) AS messageContent,
 messageCreationDate,
 post.id AS postId,
 person.id AS personId,
 person.firstName AS personFirstName,
 person.lastName AS personLastName
ORDER BY messageCreationDate DESC, messageId ASC";

const IS3: &str = "\
MATCH (n:Person {id: $personId})-[r:KNOWS]-(friend)
RETURN
    friend.id AS personId,
    friend.firstName AS firstName,
    friend.lastName AS lastName,
    r.creationDate AS friendshipCreationDate
ORDER BY friendshipCreationDate DESC, toInteger(personId) ASC";

const IS4: &str = "\
MATCH (m)
WHERE (m:Post OR m:Comment) AND m.id = $messageId
RETURN
    m.creationDate AS messageCreationDate,
    coalesce(m.content, m.imageFile) AS messageContent";

const IS5: &str = "\
MATCH (m)-[:HAS_CREATOR]->(p:Person)
WHERE (m:Post OR m:Comment) AND m.id = $messageId
RETURN
    p.id AS personId,
    p.firstName AS firstName,
    p.lastName AS lastName";

const IS6: &str = "\
MATCH (m)-[:REPLY_OF*0..]->(p:Post)<-[:CONTAINER_OF]-(f:Forum)-[:HAS_MODERATOR]->(mod:Person)
WHERE (m:Post OR m:Comment) AND m.id = $messageId
RETURN
    f.id AS forumId,
    f.title AS forumTitle,
    mod.id AS moderatorId,
    mod.firstName AS moderatorFirstName,
    mod.lastName AS moderatorLastName";

const IS7: &str = "\
MATCH (m)<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person)
WHERE (m:Post OR m:Comment) AND m.id = $messageId
OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)
RETURN c.id AS commentId,
    c.content AS commentContent,
    c.creationDate AS commentCreationDate,
    p.id AS replyAuthorId,
    p.firstName AS replyAuthorFirstName,
    p.lastName AS replyAuthorLastName,
    CASE WHEN r IS NULL THEN false ELSE true END AS replyAuthorKnowsOriginalMessageAuthor
ORDER BY commentCreationDate DESC, replyAuthorId";

static DEFINITIONS: [QueryDefinition; 20] = [
    QueryDefinition {
        operation: Operation::Ic1,
        interface: QueryInterface::Cypher(IC1),
        parameters: &[int("personId"), text("firstName")],
        columns: &[
            "friendId",
            "friendLastName",
            "distanceFromPerson",
            "friendBirthday",
            "friendCreationDate",
            "friendGender",
            "friendBrowserUsed",
            "friendLocationIp",
            "friendEmails",
            "friendLanguages",
            "friendCityName",
            "friendUniversities",
            "friendCompanies",
        ],
        unordered_list_columns: &["friendUniversities", "friendCompanies"],
        limit: Some(20),
        notes: "shortestPath((p)-[:KNOWS*1..3]-(friend)) becomes min(length(path)) over the \
                bounded match; university and company tuples are maps \
                {name, classYear|workFrom, city|country} because GraphForge encodes \
                heterogeneous lists as tagged unions; CASE x WHEN null becomes CASE WHEN x IS NULL",
    },
    QueryDefinition {
        operation: Operation::Ic2,
        interface: QueryInterface::Cypher(IC2),
        parameters: &[int("personId"), int("maxDate")],
        columns: &[
            "personId",
            "personFirstName",
            "personLastName",
            "postOrCommentId",
            "postOrCommentContent",
            "postOrCommentCreationDate",
        ],
        unordered_list_columns: &[],
        limit: Some(20),
        notes: ":Message becomes (message:Post OR message:Comment); the reference keeps \
                creationDate <= $maxDate although the specification prose says before $maxDate",
    },
    QueryDefinition {
        operation: Operation::Ic3,
        interface: QueryInterface::Cypher(IC3),
        parameters: &[
            int("personId"),
            text("countryXName"),
            text("countryYName"),
            int("startDate"),
            int("endDate"),
        ],
        columns: &[
            "friendId",
            "friendFirstName",
            "friendLastName",
            "xCount",
            "yCount",
            "xyCount",
        ],
        unordered_list_columns: &[],
        limit: Some(20),
        notes: "node-list membership (country IN [countryX, countryY], city IN cities) and \
                node equality are expressed on the unique Place id; the reference's endDate \
                parameter replaces the specification's durationDays",
    },
    QueryDefinition {
        operation: Operation::Ic4,
        interface: QueryInterface::Cypher(IC4),
        parameters: &[int("personId"), int("startDate"), int("endDate")],
        columns: &["tagName", "postCount"],
        unordered_list_columns: &[],
        limit: Some(10),
        notes: "reference text unchanged; endDate replaces the specification's durationDays",
    },
    QueryDefinition {
        operation: Operation::Ic5,
        interface: QueryInterface::Cypher(IC5),
        parameters: &[int("personId"), int("minDate")],
        columns: &["forumName", "postCount"],
        unordered_list_columns: &[],
        limit: Some(20),
        notes: "friend IN friends is expressed on the unique Person id, and the filtered \
                OPTIONAL MATCH count becomes a conditional sum over the forum's posts (GraphForge \
                rejects that OPTIONAL MATCH ... WHERE with an unbound-variable plan error)",
    },
    QueryDefinition {
        operation: Operation::Ic6,
        interface: QueryInterface::Cypher(IC6),
        parameters: &[int("personId"), text("tagName")],
        columns: &["tagName", "postCount"],
        unordered_list_columns: &[],
        limit: Some(10),
        notes: "reference text unchanged",
    },
    QueryDefinition {
        operation: Operation::Ic7,
        interface: QueryInterface::Cypher(IC7),
        parameters: &[int("personId")],
        columns: &[
            "personId",
            "personFirstName",
            "personLastName",
            "likeCreationDate",
            "commentOrPostId",
            "commentOrPostContent",
            "minutesLatency",
            "isNew",
        ],
        unordered_list_columns: &[],
        limit: Some(20),
        notes: "head(collect({msg, likeTime})) after ORDER BY likeTime DESC, message.id ASC is \
                computed as max(likeTime) then min(message.id) among likes at that time, which \
                selects the same like without relying on aggregation input order; \
                not((liker)-[:KNOWS]-(person)) becomes an empty pattern comprehension; \
                :Message becomes (message:Post OR message:Comment)",
    },
    QueryDefinition {
        operation: Operation::Ic8,
        interface: QueryInterface::Cypher(IC8),
        parameters: &[int("personId")],
        columns: &[
            "personId",
            "personFirstName",
            "personLastName",
            "commentCreationDate",
            "commentId",
            "commentContent",
        ],
        unordered_list_columns: &[],
        limit: Some(20),
        notes: "(:Message) becomes a named node with (message:Post OR message:Comment)",
    },
    QueryDefinition {
        operation: Operation::Ic9,
        interface: QueryInterface::Cypher(IC9),
        parameters: &[int("personId"), int("maxDate")],
        columns: &[
            "personId",
            "personFirstName",
            "personLastName",
            "commentOrPostId",
            "commentOrPostContent",
            "commentOrPostCreationDate",
        ],
        unordered_list_columns: &[],
        limit: Some(20),
        notes: "ORDER BY message.id becomes its projected alias commentOrPostId; :Message \
                becomes (message:Post OR message:Comment)",
    },
    QueryDefinition {
        operation: Operation::Ic10,
        interface: QueryInterface::Cypher(IC10),
        parameters: &[int("personId"), int("month")],
        columns: &[
            "personId",
            "personFirstName",
            "personLastName",
            "commonInterestScore",
            "personGender",
            "personCityName",
        ],
        unordered_list_columns: &[],
        limit: Some(10),
        notes: "datetime({epochMillis: birthday}).month/.day is computed with exact integer \
                civil-from-days arithmetic in UTC; the post list comprehension with a pattern \
                predicate becomes an OPTIONAL MATCH post count and a distinct count of posts \
                matching the interest pattern (GraphForge ignores an EXISTS predicate in an \
                OPTIONAL MATCH ... WHERE)",
    },
    QueryDefinition {
        operation: Operation::Ic11,
        interface: QueryInterface::Cypher(IC11),
        parameters: &[int("personId"), text("countryName"), int("workFromYear")],
        columns: &[
            "personId",
            "personFirstName",
            "personLastName",
            "organizationName",
            "organizationWorkFromYear",
        ],
        unordered_list_columns: &[],
        limit: Some(10),
        notes: "reference text unchanged",
    },
    QueryDefinition {
        operation: Operation::Ic12,
        interface: QueryInterface::Cypher(IC12),
        parameters: &[int("personId"), text("tagClassName")],
        columns: &[
            "personId",
            "personFirstName",
            "personLastName",
            "tagNames",
            "replyCount",
        ],
        unordered_list_columns: &["tagNames"],
        limit: Some(20),
        notes: "[:HAS_TYPE|IS_SUBCLASS_OF*0..] becomes [:HAS_TYPE] then [:IS_SUBCLASS_OF*0..], \
                the only paths the SNB schema admits from a Tag to a TagClass; the reference's \
                tag.name = $tagClassName disjunct is kept",
    },
    QueryDefinition {
        operation: Operation::Ic13,
        interface: QueryInterface::BfsPathLength {
            label: "Person",
            id_property: "id",
            relationship_type: "KNOWS",
            source_parameter: "person1Id",
            target_parameter: "person2Id",
        },
        parameters: &[int("person1Id"), int("person2Id")],
        columns: &["shortestPathLength"],
        unordered_list_columns: &[],
        limit: None,
        notes: "paths(by=bfs, via=KNOWS, directed=false) instead of Cypher shortestPath; \
                -1 when the verb returns no row, 0 for equal endpoints as the specification states",
    },
    QueryDefinition {
        operation: Operation::Is1,
        interface: QueryInterface::Cypher(IS1),
        parameters: &[int("personId")],
        columns: &[
            "firstName",
            "lastName",
            "birthday",
            "locationIP",
            "browserUsed",
            "cityId",
            "gender",
            "creationDate",
        ],
        unordered_list_columns: &[],
        limit: None,
        notes: "reference text unchanged",
    },
    QueryDefinition {
        operation: Operation::Is2,
        interface: QueryInterface::Cypher(IS2),
        parameters: &[int("personId")],
        columns: &[
            "messageId",
            "messageContent",
            "messageCreationDate",
            "postId",
            "personId",
            "personFirstName",
            "personLastName",
        ],
        unordered_list_columns: &[],
        limit: Some(10),
        notes: "reference text unchanged; it breaks creation-date ties by messageId ascending \
                although the specification lists descending",
    },
    QueryDefinition {
        operation: Operation::Is3,
        interface: QueryInterface::Cypher(IS3),
        parameters: &[int("personId")],
        columns: &[
            "personId",
            "firstName",
            "lastName",
            "friendshipCreationDate",
        ],
        unordered_list_columns: &[],
        limit: None,
        notes: "reference text unchanged",
    },
    QueryDefinition {
        operation: Operation::Is4,
        interface: QueryInterface::Cypher(IS4),
        parameters: &[int("messageId")],
        columns: &["messageCreationDate", "messageContent"],
        unordered_list_columns: &[],
        limit: None,
        notes: "(m:Message {id}) becomes (m) WHERE (m:Post OR m:Comment) AND m.id",
    },
    QueryDefinition {
        operation: Operation::Is5,
        interface: QueryInterface::Cypher(IS5),
        parameters: &[int("messageId")],
        columns: &["personId", "firstName", "lastName"],
        unordered_list_columns: &[],
        limit: None,
        notes: "(m:Message {id}) becomes (m) WHERE (m:Post OR m:Comment) AND m.id",
    },
    QueryDefinition {
        operation: Operation::Is6,
        interface: QueryInterface::Cypher(IS6),
        parameters: &[int("messageId")],
        columns: &[
            "forumId",
            "forumTitle",
            "moderatorId",
            "moderatorFirstName",
            "moderatorLastName",
        ],
        unordered_list_columns: &[],
        limit: None,
        notes: "(m:Message {id}) becomes (m) WHERE (m:Post OR m:Comment) AND m.id",
    },
    QueryDefinition {
        operation: Operation::Is7,
        interface: QueryInterface::Cypher(IS7),
        parameters: &[int("messageId")],
        columns: &[
            "commentId",
            "commentContent",
            "commentCreationDate",
            "replyAuthorId",
            "replyAuthorFirstName",
            "replyAuthorLastName",
            "replyAuthorKnowsOriginalMessageAuthor",
        ],
        unordered_list_columns: &[],
        limit: None,
        notes: "CASE r WHEN null THEN false ELSE true END becomes CASE WHEN r IS NULL, the \
                specification's knows flag; :Message {id} becomes (m:Post OR m:Comment) AND m.id",
    },
];

/// Every runnable SNB Interactive read (IC1–IC13, IS1–IS7), in operation order.
pub fn query_definitions() -> &'static [QueryDefinition] {
    &DEFINITIONS
}

/// The runnable definition for `operation`, or `None` for a refused operation.
pub fn query_definition(operation: Operation) -> Option<&'static QueryDefinition> {
    DEFINITIONS
        .iter()
        .find(|definition| definition.operation == operation)
}
