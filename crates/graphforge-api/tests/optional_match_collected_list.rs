//! A subquery that reads a list collected in an earlier `WITH` (`OPTIONAL
//! MATCH ... WHERE x IN <list>`, the LDBC SNB BI13 and Interactive IC5 shape,
//! and `EXISTS { ... WHERE x IN <list> }`) runs once per outer row, seeded with
//! that row (#1887 D15). The seed must be the very rows the subquery joins back
//! to: a `collect` is free to return its list in a different order each time it
//! is evaluated, so seeding from a second evaluation of the outer plan lost
//! every row whose list held more than one element (#1919).

use std::collections::BTreeMap;

use arrow::array::Array;
use arrow::util::display::array_value_to_string;
use graphforge_api::GraphForge;

fn rows(gf: &GraphForge, query: &str) -> Vec<Vec<String>> {
    let result = gf
        .execute(query)
        .unwrap_or_else(|error| panic!("{query}: {error}"));
    let mut rows = Vec::new();
    for batch in &result.batches {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|array| {
                        if array.is_null(row) {
                            "null".to_owned()
                        } else {
                            array_value_to_string(array, row).expect("render")
                        }
                    })
                    .collect(),
            );
        }
    }
    rows
}

fn pairs(rows: Vec<Vec<String>>) -> BTreeMap<i64, i64> {
    rows.into_iter()
        .map(|row| (row[0].parse().expect("key"), row[1].parse().expect("count")))
        .collect()
}

#[test]
fn correlated_optional_match_preserves_null_collected_node_seeds() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    gf.execute("CREATE (:Person {id: 1})")
        .expect("create person");

    let actual = rows(
        &gf,
        "MATCH (p:Person)
         WITH collect(p) AS people
         UNWIND [people[0], people[99]] AS person
         OPTIONAL MATCH (person)-[:KNOWS]->(friend)
         WHERE friend IN people
         RETURN person.id, friend.id",
    );
    let mut actual = actual;
    actual.sort();
    assert_eq!(
        actual,
        vec![
            vec!["1".to_owned(), "null".to_owned()],
            vec!["null".to_owned(), "null".to_owned()]
        ],
    );
}

const PERSONS: i64 = 60;
const FORUMS: i64 = 9;
const POSTS: i64 = 200;

/// Person 0 knows persons 1..=60. Forum `f` has person `p` as a member when
/// `(7p + 3f) % 5 < 3`; forum 9 has no posts. Post `o` sits in forum
/// `o % 8 + 1` and was created by person `o % 60 + 1`.
fn forum_graph() -> GraphForge {
    let gf = GraphForge::new(None).expect("in-memory instance");
    for statement in [
        "UNWIND range(0, 60) AS i CREATE (:Person {id: i})",
        "UNWIND range(1, 9) AS i CREATE (:Forum {id: i})",
        "UNWIND range(1, 200) AS i CREATE (:Post {id: i})",
        "MATCH (a:Person {id: 0}), (b:Person) WHERE b.id > 0 CREATE (a)-[:KNOWS]->(b)",
        "MATCH (p:Person), (f:Forum) WHERE (p.id * 7 + f.id * 3) % 5 < 3
         CREATE (f)-[:HAS_MEMBER]->(p)",
        "MATCH (o:Post), (p:Person) WHERE o.id % 60 + 1 = p.id CREATE (o)-[:HAS_CREATOR]->(p)",
        "MATCH (o:Post), (f:Forum) WHERE o.id % 8 + 1 = f.id CREATE (f)-[:CONTAINER_OF]->(o)",
    ] {
        gf.execute(statement)
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    gf
}

/// The posts of each forum created by one of the forum's members.
fn expected_member_posts() -> BTreeMap<i64, i64> {
    (1..=FORUMS)
        .map(|forum| {
            let count = (1..=POSTS)
                .filter(|post| {
                    let creator = post % PERSONS + 1;
                    post % 8 + 1 == forum && (creator * 7 + forum * 3) % 5 < 3
                })
                .count();
            (forum, i64::try_from(count).expect("count"))
        })
        .collect()
}

/// Every forum, with the members of person 0's friends collected per forum.
const FORUM_FRIENDS: &str = "MATCH (:Person {id: 0})-[:KNOWS]-(friend)
     WITH DISTINCT friend
     MATCH (friend)<-[:HAS_MEMBER]-(forum:Forum)
     WITH forum, collect(friend) AS friends";

#[test]
fn optional_match_where_in_collected_list_counts_the_listed_creators() {
    let gf = forum_graph();
    let expected = expected_member_posts();
    assert!(
        expected.values().filter(|count| **count > 0).count() > 4
            && expected.values().any(|count| *count == 0),
        "the data must mix forums with and without matching posts: {expected:?}"
    );

    // The LDBC IC5 text.
    let optional = pairs(rows(
        &gf,
        &format!(
            "{FORUM_FRIENDS}
             OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)
             WHERE friend IN friends
             WITH forum, count(post) AS postCount
             RETURN forum.id, postCount"
        ),
    ));
    assert_eq!(optional, expected);

    // The conditional-sum form it was rewritten to while this was broken.
    let conditional = pairs(rows(
        &gf,
        &format!(
            "{FORUM_FRIENDS}
             OPTIONAL MATCH (forum)-[:CONTAINER_OF]->(post)-[:HAS_CREATOR]->(author)
             WITH forum,
                  sum(CASE WHEN post IS NOT NULL AND author IN friends THEN 1 ELSE 0 END) AS c
             RETURN forum.id, c"
        ),
    ));
    assert_eq!(conditional, expected);
}

#[test]
fn optional_match_where_in_collected_list_keeps_the_projection_after_it() {
    let gf = forum_graph();
    let mut ranked: Vec<(i64, i64)> = expected_member_posts().into_iter().collect();
    ranked.sort_by_key(|(forum, count)| (-count, *forum));
    ranked.truncate(5);

    let top: Vec<(i64, i64)> = rows(
        &gf,
        &format!(
            "{FORUM_FRIENDS}
             OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)
             WHERE friend IN friends
             WITH forum, count(post) AS postCount
             RETURN forum.id, postCount
             ORDER BY postCount DESC, forum.id ASC
             LIMIT 5"
        ),
    )
    .into_iter()
    .map(|row| {
        (
            row[0].parse().expect("forum"),
            row[1].parse().expect("count"),
        )
    })
    .collect();
    assert_eq!(top, ranked);
}

#[test]
fn exists_where_in_collected_list_selects_the_forums_with_listed_creators() {
    let gf = forum_graph();
    let expected = expected_member_posts();
    let with_posts: Vec<i64> = expected
        .iter()
        .filter(|(_, count)| **count > 0)
        .map(|(forum, _)| *forum)
        .collect();
    let selected = || -> Vec<i64> {
        rows(
            &gf,
            &format!(
                "{FORUM_FRIENDS}
                 MATCH (forum)
                 WHERE EXISTS {{ (author)<-[:HAS_CREATOR]-(:Post)<-[:CONTAINER_OF]-(forum)
                                      WHERE author IN friends }}
                 RETURN forum.id ORDER BY forum.id"
            ),
        )
        .into_iter()
        .map(|row| row[0].parse().expect("forum"))
        .collect()
    };
    assert_eq!(selected(), with_posts);
}

/// The SNB BI13 shape: the collected list is unwound, and every unwound row
/// carries the whole list into the `OPTIONAL MATCH ... WHERE x IN <list>`. A
/// second `OPTIONAL MATCH` on the same variable then counts every like, also
/// for the zombies whose first one matched nothing.
#[test]
fn optional_match_where_in_unwound_collected_list_counts_likes_by_listed_persons() {
    let gf = GraphForge::new(None).expect("in-memory instance");
    for statement in [
        "UNWIND range(1, 40) AS i CREATE (:Person {id: i})",
        "UNWIND range(1, 120) AS i CREATE (:Message {id: i})",
        "MATCH (m:Message), (p:Person) WHERE m.id % 40 + 1 = p.id CREATE (m)-[:HAS_CREATOR]->(p)",
        "MATCH (m:Message), (p:Person) WHERE (m.id * 5 + p.id * 3) % 19 < 2
         CREATE (p)-[:LIKES]->(m)",
    ] {
        gf.execute(statement)
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    // Zombies are the persons whose id is divisible by 3. Message `m` was
    // created by person `m % 40 + 1`, and person `p` likes it when
    // `(5m + 3p) % 19 < 2`.
    let likes = |zombie: i64, by_zombies_only: bool| -> i64 {
        let count = (1..=120_i64)
            .filter(|message| message % 40 + 1 == zombie)
            .flat_map(|message| (1..=40_i64).map(move |liker| (message, liker)))
            .filter(|(message, liker)| {
                (message * 5 + liker * 3) % 19 < 2 && (!by_zombies_only || liker % 3 == 0)
            })
            .count();
        i64::try_from(count).expect("count")
    };
    let expected: BTreeMap<i64, (i64, i64)> = (1..=40_i64)
        .filter(|zombie| zombie % 3 == 0)
        .map(|zombie| (zombie, (likes(zombie, true), likes(zombie, false))))
        .collect();
    assert!(
        expected.values().any(|(by_zombies, _)| *by_zombies > 1)
            && expected
                .values()
                .any(|(by_zombies, total)| *by_zombies == 0 && *total > 0)
            && expected
                .values()
                .all(|(by_zombies, total)| by_zombies < total),
        "{expected:?}"
    );

    let counted: BTreeMap<i64, (i64, i64)> = rows(
        &gf,
        "MATCH (zombie:Person) WHERE zombie.id % 3 = 0
         WITH collect(zombie) AS zombies
         UNWIND zombies AS zombie
         OPTIONAL MATCH (zombie)<-[:HAS_CREATOR]-(message)<-[:LIKES]-(likerZombie:Person)
         WHERE likerZombie IN zombies
         WITH zombie, count(likerZombie) AS zombieLikeCount
         OPTIONAL MATCH (zombie)<-[:HAS_CREATOR]-(message)<-[:LIKES]-(likerPerson:Person)
         WITH zombie, zombieLikeCount, count(likerPerson) AS totalLikeCount
         RETURN zombie.id, zombieLikeCount, totalLikeCount",
    )
    .into_iter()
    .map(|row| {
        (
            row[0].parse().expect("zombie"),
            (
                row[1].parse().expect("zombie likes"),
                row[2].parse().expect("total likes"),
            ),
        )
    })
    .collect();
    assert_eq!(counted, expected);
}
