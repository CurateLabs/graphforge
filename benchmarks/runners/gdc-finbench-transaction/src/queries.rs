//! FinBench Transaction read queries as data: Cypher text, parameters, result
//! columns, ordering and truncation, for every read the public Cypher surface
//! answers exactly.
//!
//! Shared semantics (LDBC FinBench Transaction specification, pinned in the
//! live identity):
//!
//! * Time windows are open: `startTime < timestamp < endTime`.
//! * Amount thresholds are strict: `amount > threshold`.
//! * Calculated floats are rounded to 3 decimal places.
//! * **Truncation** (`truncationLimit`, `truncationOrder`): when a step expands
//!   from a vertex, only the `truncationLimit` newest edges of the expanded
//!   type and direction at that vertex are traversed. Truncation applies to the
//!   vertex's whole adjacency, before the window and amount filters, so it is a
//!   property of the vertex and not of the path that reached it. Ties on
//!   `timestamp` are broken by the far endpoint's id, ascending; edges that
//!   share both are kept or dropped together. The specification leaves ties
//!   undefined, so this is a documented variance ([`TIE_BREAK_VARIANCE`],
//!   carried by every [`Truncation`]). Only `TIMESTAMP_DESCENDING`, the
//!   order the LDBC parameter generator emits, is supported. Another order is a
//!   typed refusal, never a silent reorder.
//!
//! In Cypher, a step's truncation is the per-vertex list of
//! `[farId, timestamp]` keys ordered newest first and sliced to the limit; an
//! edge is traversed only if its key is in that slice. Multi-hop reads first
//! collect the admissible `[srcId, dstId, timestamp]` keys of every vertex the
//! traversal can expand from, then require every hop of a path to be admissible.
//!
//! The scorecard's reference is spec-derived: the independent
//! `graphforge_bench.gdc_finbench_transaction_reference` module, run at SF1
//! (decision on #952). Where GPStore, an LDBC reference implementation, reads
//! the specification differently, `reference_reading` records that for
//! information only; `workarounds` names the GraphForge gap (#1888) or the declared
//! variance behind any departure from the most direct Cypher.

use crate::{Operation, SuiteError, ValidationMode};
use graphforge_api::IrLiteral;
use serde::Serialize;
use std::collections::HashMap;

/// Schema of the serialized query catalog (`list-queries`).
pub const QUERY_CATALOG_SCHEMA: &str = "graphforge-gdc-finbench-query-catalog/1";
/// Default `truncationLimit` emitted by the LDBC FinBench parameter generator.
pub const DEFAULT_TRUNCATION_LIMIT: i64 = 500;
/// The only `truncationOrder` the queries implement.
pub const TRUNCATION_ORDER: &str = "TIMESTAMP_DESCENDING";
/// Typed cause for a binding that asks for another truncation order.
pub const TRUNCATION_ORDER_CAUSE: &str = "truncation_order_not_supported";

/// The value kind of a query parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParameterKind {
    /// An entity id (`Int64`).
    Id,
    /// A `DateTime` as integer epoch milliseconds (`Int64`).
    EpochMillis,
    /// A `Float64` threshold.
    Float,
    /// The truncation limit (`Int64`).
    TruncationLimit,
    /// The truncation order enum; validated, never sent to Cypher.
    TruncationOrder,
}

/// The value kind of a result column, which also fixes its text form.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnKind {
    /// `Int64`, printed in decimal.
    Int,
    /// `Float64` rounded to 3 decimals, printed with exactly 3 decimals.
    Float3,
    /// `Boolean`, printed `true` / `false`.
    Bool,
    /// `Utf8`, printed as is.
    Text,
    /// `List(Int64)`, printed `[a, b, c]`.
    IntList,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct Parameter {
    pub name: &'static str,
    pub kind: ParameterKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct Column {
    pub name: &'static str,
    pub kind: ColumnKind,
}

/// How a read applies `truncationLimit`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct Truncation {
    pub limit_parameter: &'static str,
    pub order_parameter: &'static str,
    pub default_limit: i64,
    pub order: &'static str,
    /// Every expansion the limit applies to, as `edge type, direction, from`.
    pub truncated_steps: &'static [&'static str],
    /// How edges with equal `timestamp` at the cut-off are ordered. The
    /// specification leaves this undefined, so it is a documented variance.
    pub tie_break_variance: &'static str,
}

/// The accepted variance (#952, 2026-10-07) for ties at the truncation cut-off.
pub const TIE_BREAK_VARIANCE: &str = "variance from the specification, which leaves ties \
undefined: edges with equal timestamp at the cut-off are ordered by far-endpoint id ascending; \
edges sharing both timestamp and far endpoint are kept or dropped together";

/// One runnable FinBench Transaction read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct QueryDefinition {
    pub operation: Operation,
    pub cypher: &'static str,
    pub parameters: &'static [Parameter],
    pub columns: &'static [Column],
    /// `exact` when the specification orders every row; `normalized` when it
    /// leaves ties unordered.
    pub validation: &'static str,
    pub truncation: Option<Truncation>,
    /// The specification reading this Cypher implements.
    pub semantics: &'static str,
    /// Informational: where GPStore, an LDBC reference implementation, reads
    /// the specification differently, how it does and what this reference
    /// follows. The scorecard checks against the spec-derived reference
    /// (`graphforge_bench.gdc_finbench_transaction_reference`), not GPStore.
    pub reference_reading: Option<&'static str>,
    /// Why the Cypher departs from the most direct form: each entry names the
    /// GraphForge defect or gap it works around.
    pub workarounds: &'static [&'static str],
}

impl QueryDefinition {
    pub fn validation_mode(&self) -> ValidationMode {
        if self.validation == ValidationMode::Normalized.name() {
            ValidationMode::Normalized
        } else {
            ValidationMode::Exact
        }
    }

    /// Bind one parameter set (JSON object) to the query's Cypher parameters.
    ///
    /// Every declared parameter must be present and no other may be. A
    /// `truncationOrder` other than [`TRUNCATION_ORDER`] is a typed refusal.
    pub fn bind(
        &self,
        binding: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<HashMap<String, IrLiteral>, SuiteError> {
        let operation = self.operation;
        for name in binding.keys() {
            if !self
                .parameters
                .iter()
                .any(|parameter| parameter.name == name)
            {
                return Err(SuiteError::InvalidDocument(format!(
                    "{operation} binding has undeclared parameter {name}"
                )));
            }
        }
        let mut params = HashMap::new();
        for parameter in self.parameters {
            let value = binding.get(parameter.name).ok_or_else(|| {
                SuiteError::InvalidDocument(format!(
                    "{operation} binding is missing parameter {}",
                    parameter.name
                ))
            })?;
            let literal = match parameter.kind {
                ParameterKind::Id | ParameterKind::EpochMillis | ParameterKind::TruncationLimit => {
                    value.as_i64().map(IrLiteral::Int)
                }
                ParameterKind::Float => value.as_f64().map(IrLiteral::Float),
                ParameterKind::TruncationOrder => {
                    if value.as_str() != Some(TRUNCATION_ORDER) {
                        return Err(SuiteError::SemanticIncompatibility {
                            cause: TRUNCATION_ORDER_CAUSE.into(),
                            detail: format!(
                                "{operation} implements only {TRUNCATION_ORDER} truncation; got {value}"
                            ),
                        });
                    }
                    continue;
                }
            }
            .ok_or_else(|| {
                SuiteError::InvalidDocument(format!(
                    "{operation} parameter {} must be {:?}, got {value}",
                    parameter.name, parameter.kind
                ))
            })?;
            if parameter.kind == ParameterKind::TruncationLimit
                && matches!(literal, IrLiteral::Int(limit) if limit < 1)
            {
                return Err(SuiteError::InvalidDocument(format!(
                    "{operation} truncationLimit must be positive, got {value}"
                )));
            }
            params.insert(parameter.name.to_string(), literal);
        }
        Ok(params)
    }
}

/// The serialized catalog the durable query driver iterates.
#[derive(Clone, Debug, Serialize)]
pub struct QueryCatalog {
    pub schema: &'static str,
    pub suite_id: &'static str,
    pub queries: Vec<QueryDefinition>,
}

pub fn query_catalog() -> QueryCatalog {
    QueryCatalog {
        schema: QUERY_CATALOG_SCHEMA,
        suite_id: crate::SUITE_ID,
        queries: QUERIES.to_vec(),
    }
}

/// The definition for `operation`, or `None` when the read is refused.
pub fn query_definition(operation: Operation) -> Option<&'static QueryDefinition> {
    QUERIES.iter().find(|query| query.operation == operation)
}

const fn p(name: &'static str, kind: ParameterKind) -> Parameter {
    Parameter { name, kind }
}

const fn c(name: &'static str, kind: ColumnKind) -> Column {
    Column { name, kind }
}

const ID: Parameter = p("id", ParameterKind::Id);
const START: Parameter = p("startTime", ParameterKind::EpochMillis);
const END: Parameter = p("endTime", ParameterKind::EpochMillis);
const LIMIT: Parameter = p("truncationLimit", ParameterKind::TruncationLimit);
const ORDER: Parameter = p("truncationOrder", ParameterKind::TruncationOrder);

const fn truncation(truncated_steps: &'static [&'static str]) -> Option<Truncation> {
    Some(Truncation {
        limit_parameter: "truncationLimit",
        order_parameter: "truncationOrder",
        default_limit: DEFAULT_TRUNCATION_LIMIT,
        order: TRUNCATION_ORDER,
        truncated_steps,
        tie_break_variance: TIE_BREAK_VARIANCE,
    })
}

/// Truncation and path rules compare `[id, id, timestamp]` keys, not edges.
const W_EDGE_KEYS: &str = "edges are identified by [endpoint id, timestamp] keys because the \
declared tie-break variance keeps or drops edges sharing both together, which an edge \
identity would not, and startNode/endNode/id are unsupported for path hops (#1888)";
/// Path hop rules use a list comprehension in a second `WITH`.
const W_HOP_LIST: &str = "hop rules are a list comprehension over id and timestamp lists in a \
separate WITH because ALL(i IN range(...)) over path relationships, a path-node comprehension \
with WHERE in the same WITH, and reduce() fail or are unsupported (#1888)";
/// Optional truncated steps aggregate admitted rows.
const W_OPTIONAL_WHERE: &str = "truncated optional steps keep every OPTIONAL MATCH row and \
aggregate only CASE-admitted ones because OPTIONAL MATCH ... WHERE cannot reference a WITH \
variable (#1888 D7)";
/// No shortestPath.
const W_SHORTEST_PATH: &str = "an unbounded variable-length match with min(size(r)) stands in for \
shortestPath, which is unsupported (#1888)";
/// No multi-type variable-length relationship.
const W_MULTI_TYPE: &str = "an untyped variable-length match filtered by type(e) stands in for \
[:transfer|withdraw*1..3], which fails at execution (#1888)";

const EXACT: &str = "exact";
const NORMALIZED: &str = "normalized";

/// Rejects a path (`ids`, `ts`) with a hop that is not admissible, outside the
/// window, or not strictly later than the hop before it.
macro_rules! ascending_admissible_hops {
    () => {
        "size([i IN range(0, size(ts) - 1) WHERE NOT ([ids[i], ids[i + 1], ts[i]] IN admissible) \
         OR ts[i] <= $startTime OR ts[i] >= $endTime OR (i > 0 AND ts[i - 1] >= ts[i])]) = 0"
    };
}

const TCR1: &str = concat!(
    "MATCH (account:Account {id: $id})-[:transfer*0..2]->(v:Account) ",
    "WITH DISTINCT account, v ",
    "MATCH (v)-[e:transfer]->(w:Account) ",
    "WITH account, v, e, w ORDER BY e.timestamp DESC, w.id ASC ",
    "WITH account, v, collect([v.id, w.id, e.timestamp]) AS keys ",
    "UNWIND keys[0..$truncationLimit] AS key ",
    "WITH account, collect(key) AS admissible ",
    "MATCH p = (account)-[r:transfer*1..3]->(other:Account) ",
    "WITH admissible, other, [n IN nodes(p) | n.id] AS ids, [e IN r | e.timestamp] AS ts ",
    "WITH admissible, other, ids, ts WHERE ",
    ascending_admissible_hops!(),
    " WITH DISTINCT other, size(ts) AS accountDistance ",
    "MATCH (other)<-[s:signIn]-(medium:Medium) ",
    "WHERE medium.isBlocked = true AND $startTime < s.timestamp AND s.timestamp < $endTime ",
    "RETURN DISTINCT other.id AS otherId, accountDistance, medium.id AS mediumId, ",
    "medium.type AS mediumType ",
    "ORDER BY accountDistance ASC, otherId ASC, mediumId ASC"
);

const TCR2: &str = concat!(
    "MATCH (person:Person {id: $id})-[:own]->(:Account)<-[:transfer*0..2]-(v:Account) ",
    "WITH DISTINCT person, v ",
    "MATCH (v)<-[e:transfer]-(u:Account) ",
    "WITH person, v, e, u ORDER BY e.timestamp DESC, u.id ASC ",
    "WITH person, v, collect([u.id, v.id, e.timestamp]) AS keys ",
    "UNWIND keys[0..$truncationLimit] AS key ",
    "WITH person, collect(key) AS admissible ",
    "MATCH (person)-[:own]->(account:Account) ",
    "MATCH p = (other:Account)-[r:transfer*1..3]->(account) ",
    "WITH admissible, other, [n IN nodes(p) | n.id] AS ids, [e IN r | e.timestamp] AS ts ",
    "WITH admissible, other, ids, ts WHERE ",
    ascending_admissible_hops!(),
    " WITH DISTINCT other ",
    "MATCH (other)<-[d:deposit]-(loan:Loan) ",
    "WHERE $startTime < d.timestamp AND d.timestamp < $endTime ",
    "WITH DISTINCT other, loan ",
    "WITH other, sum(loan.loanAmount) AS amountTotal, sum(loan.balance) AS balanceTotal ",
    "RETURN other.id AS otherId, round(amountTotal * 1000) / 1000 AS sumLoanAmount, ",
    "round(balanceTotal * 1000) / 1000 AS sumLoanBalance ",
    "ORDER BY sumLoanAmount DESC, otherId ASC"
);

const TCR3: &str = concat!(
    "MATCH (src:Account {id: $id1})-[r:transfer*1..]->(dst:Account {id: $id2}) ",
    "WHERE ALL(e IN r WHERE $startTime < e.timestamp AND e.timestamp < $endTime) ",
    "RETURN coalesce(min(size(r)), -1) AS shortestPathLength"
);

const TCR4: &str = concat!(
    "MATCH (src:Account {id: $id1})-[edge1:transfer]->(dst:Account {id: $id2}) ",
    "WHERE $startTime < edge1.timestamp AND edge1.timestamp < $endTime ",
    "WITH DISTINCT src, dst ",
    "MATCH (dst)-[e3:transfer]->(other:Account)-[e2:transfer]->(src) ",
    "WHERE $startTime < e2.timestamp AND e2.timestamp < $endTime ",
    "AND $startTime < e3.timestamp AND e3.timestamp < $endTime ",
    "WITH DISTINCT src, dst, other ",
    "MATCH (other)-[edge2:transfer]->(src) ",
    "WHERE $startTime < edge2.timestamp AND edge2.timestamp < $endTime ",
    "WITH dst, other, count(edge2) AS numEdge2, sum(edge2.amount) AS sum2, ",
    "max(edge2.amount) AS max2 ",
    "MATCH (dst)-[edge3:transfer]->(other) ",
    "WHERE $startTime < edge3.timestamp AND edge3.timestamp < $endTime ",
    "WITH other, numEdge2, sum2, max2, count(edge3) AS numEdge3, sum(edge3.amount) AS sum3, ",
    "max(edge3.amount) AS max3 ",
    "RETURN other.id AS otherId, numEdge2, round(sum2 * 1000) / 1000 AS sumEdge2Amount, ",
    "round(max2 * 1000) / 1000 AS maxEdge2Amount, numEdge3, ",
    "round(sum3 * 1000) / 1000 AS sumEdge3Amount, round(max3 * 1000) / 1000 AS maxEdge3Amount ",
    "ORDER BY sumEdge2Amount DESC, sumEdge3Amount DESC, otherId ASC"
);

const TCR5: &str = concat!(
    "MATCH (person:Person {id: $id})-[:own]->(:Account)-[:transfer*0..2]->(v:Account) ",
    "WITH DISTINCT person, v ",
    "MATCH (v)-[e:transfer]->(w:Account) ",
    "WITH person, v, e, w ORDER BY e.timestamp DESC, w.id ASC ",
    "WITH person, v, collect([v.id, w.id, e.timestamp]) AS keys ",
    "UNWIND keys[0..$truncationLimit] AS key ",
    "WITH person, collect(key) AS admissible ",
    "MATCH (person)-[:own]->(src:Account) ",
    "MATCH p = (src)-[r:transfer*1..3]->(:Account) ",
    "WITH admissible, [n IN nodes(p) | n.id] AS ids, [e IN r | e.timestamp] AS ts ",
    "WITH admissible, ids, ts WHERE ",
    ascending_admissible_hops!(),
    " AND size([i IN range(0, size(ids) - 1) WHERE ids[i] IN ids[i + 1..]]) = 0 ",
    "WITH DISTINCT ids AS path ",
    "RETURN path ORDER BY size(path) DESC, path ASC"
);

const TCR6: &str = concat!(
    "MATCH (card:Account {id: $id}) WHERE card.type ENDS WITH 'card' ",
    "MATCH (card)<-[w:withdraw]-(m:Account) ",
    "WITH card, w, m ORDER BY w.timestamp DESC, m.id ASC ",
    "WITH card, collect([m.id, w.timestamp])[0..$truncationLimit] AS keptWithdraw ",
    "MATCH (card)<-[edge2:withdraw]-(mid:Account) ",
    "WHERE [mid.id, edge2.timestamp] IN keptWithdraw ",
    "AND $startTime < edge2.timestamp AND edge2.timestamp < $endTime ",
    "AND edge2.amount > $threshold2 ",
    "WITH mid, sum(edge2.amount) AS sum2 ",
    "MATCH (mid)<-[t:transfer]-(s:Account) ",
    "WITH mid, sum2, t, s ORDER BY t.timestamp DESC, s.id ASC ",
    "WITH mid, sum2, collect([s.id, t.timestamp])[0..$truncationLimit] AS keptTransfer ",
    "MATCH (mid)<-[edge1:transfer]-(src:Account) ",
    "WHERE [src.id, edge1.timestamp] IN keptTransfer ",
    "AND $startTime < edge1.timestamp AND edge1.timestamp < $endTime ",
    "AND edge1.amount > $threshold1 ",
    "WITH mid, sum2, count(edge1) AS numEdge1, sum(edge1.amount) AS sum1 ",
    "WHERE numEdge1 > 3 ",
    "RETURN mid.id AS midId, round(sum1 * 1000) / 1000 AS sumEdge1Amount, ",
    "round(sum2 * 1000) / 1000 AS sumEdge2Amount ",
    "ORDER BY sumEdge2Amount DESC, midId ASC"
);

// `OPTIONAL MATCH ... WHERE` cannot reference a variable an earlier `WITH`
// projected (GraphForge plans it as unbound, #1888 D7), so TCR7 and TCR9 keep
// every optional row of a truncated step and aggregate only the admitted ones.
const TCR7: &str = concat!(
    "MATCH (mid:Account {id: $id}) ",
    "OPTIONAL MATCH (mid)<-[t:transfer]-(u:Account) ",
    "WITH mid, t, u ORDER BY t.timestamp DESC, u.id ASC ",
    "WITH mid, collect([u.id, t.timestamp])[0..$truncationLimit] AS keptIn ",
    "OPTIONAL MATCH (mid)-[t:transfer]->(d:Account) ",
    "WITH mid, keptIn, t, d ORDER BY t.timestamp DESC, d.id ASC ",
    "WITH mid, keptIn, collect([d.id, t.timestamp])[0..$truncationLimit] AS keptOut ",
    "OPTIONAL MATCH (mid)<-[edge1:transfer]-(src:Account) ",
    "WITH mid, keptOut, src, edge1, ([src.id, edge1.timestamp] IN keptIn ",
    "AND $startTime < edge1.timestamp AND edge1.timestamp < $endTime ",
    "AND edge1.amount > $threshold) AS admitted ",
    "WITH mid, keptOut, count(DISTINCT CASE WHEN admitted THEN src.id END) AS numSrc, ",
    "sum(CASE WHEN admitted THEN edge1.amount END) AS sumIn ",
    "OPTIONAL MATCH (mid)-[edge2:transfer]->(dst:Account) ",
    "WITH numSrc, sumIn, dst, edge2, ([dst.id, edge2.timestamp] IN keptOut ",
    "AND $startTime < edge2.timestamp AND edge2.timestamp < $endTime ",
    "AND edge2.amount > $threshold) AS admitted ",
    "WITH numSrc, sumIn, count(DISTINCT CASE WHEN admitted THEN dst.id END) AS numDst, ",
    "count(CASE WHEN admitted THEN 1 END) AS numEdge2, ",
    "sum(CASE WHEN admitted THEN edge2.amount END) AS sumOut ",
    "RETURN numSrc, numDst, CASE WHEN numEdge2 = 0 THEN -1.0 ",
    "ELSE round(sumIn / sumOut * 1000) / 1000 END AS inOutRatio"
);

const TCR8: &str = concat!(
    "MATCH (loan:Loan {id: $id})-[d:deposit]->(src:Account) ",
    "WHERE $startTime < d.timestamp AND d.timestamp < $endTime ",
    "MATCH (src)-[*0..2]->(v:Account) ",
    "WITH DISTINCT loan, v ",
    "OPTIONAL MATCH (v)<-[i:transfer]-(:Account) ",
    "WHERE $startTime < i.timestamp AND i.timestamp < $endTime ",
    "WITH loan, v, sum(i.amount) AS upstream ",
    "MATCH (v)-[e]->(w:Account) WHERE type(e) IN ['transfer', 'withdraw'] ",
    "WITH loan, v, upstream, e, w ORDER BY e.timestamp DESC, w.id ASC ",
    "WITH loan, v, upstream, collect([w.id, e.timestamp])[0..$truncationLimit] AS kept ",
    "MATCH (v)-[e]->(w:Account) ",
    "WHERE type(e) IN ['transfer', 'withdraw'] AND [w.id, e.timestamp] IN kept ",
    "AND $startTime < e.timestamp AND e.timestamp < $endTime ",
    "AND e.amount > $threshold * upstream ",
    "WITH loan, collect([v.id, w.id, e.timestamp]) AS qualifying ",
    "MATCH (loan)-[d:deposit]->(src:Account) ",
    "WHERE $startTime < d.timestamp AND d.timestamp < $endTime ",
    "MATCH p = (src)-[r*1..3]->(dst:Account) ",
    "WITH loan, qualifying, dst, [n IN nodes(p) | n.id] AS ids, ",
    "[e IN r | e.timestamp] AS ts, [e IN r | e.amount] AS amounts, [e IN r | type(e)] AS types ",
    "WITH loan, qualifying, dst, ids, ts, amounts, types ",
    "WHERE size([t IN types WHERE NOT t IN ['transfer', 'withdraw']]) = 0 ",
    "AND size([i IN range(0, size(ts) - 1) ",
    "WHERE NOT ([ids[i], ids[i + 1], ts[i]] IN qualifying)]) = 0 ",
    "WITH loan, dst, size(ts) AS hops, ",
    "[toFloat(ids[size(ids) - 2]), toFloat(ts[size(ts) - 1]), amounts[size(amounts) - 1]] AS lastEdge ",
    "WITH loan, dst, min(hops) AS minHops, collect(DISTINCT lastEdge) AS lastEdges ",
    "UNWIND lastEdges AS lastEdge ",
    "WITH loan, dst, minHops, sum(lastEdge[2]) AS inflow ",
    "RETURN dst.id AS dstId, round(inflow / loan.loanAmount * 1000) / 1000 AS ratio, ",
    "minHops + 1 AS minDistanceFromLoan ",
    "ORDER BY minDistanceFromLoan DESC, ratio DESC, dstId ASC"
);

const TCR9: &str = concat!(
    "MATCH (mid:Account {id: $id}) ",
    "OPTIONAL MATCH (mid)<-[t:transfer]-(u:Account) ",
    "WITH mid, t, u ORDER BY t.timestamp DESC, u.id ASC ",
    "WITH mid, collect([u.id, t.timestamp])[0..$truncationLimit] AS keptIn ",
    "OPTIONAL MATCH (mid)-[t:transfer]->(d:Account) ",
    "WITH mid, keptIn, t, d ORDER BY t.timestamp DESC, d.id ASC ",
    "WITH mid, keptIn, collect([d.id, t.timestamp])[0..$truncationLimit] AS keptOut ",
    "OPTIONAL MATCH (mid)<-[edge1:deposit]-(:Loan) ",
    "WHERE edge1.amount > $threshold ",
    "AND $startTime < edge1.timestamp AND edge1.timestamp < $endTime ",
    "WITH mid, keptIn, keptOut, sum(edge1.amount) AS sum1 ",
    "OPTIONAL MATCH (mid)-[edge2:repay]->(:Loan) ",
    "WHERE edge2.amount > $threshold ",
    "AND $startTime < edge2.timestamp AND edge2.timestamp < $endTime ",
    "WITH mid, keptIn, keptOut, sum1, count(edge2) AS numEdge2, sum(edge2.amount) AS sum2 ",
    "OPTIONAL MATCH (mid)<-[edge3:transfer]-(up:Account) ",
    "WITH mid, keptOut, sum1, numEdge2, sum2, edge3, ([up.id, edge3.timestamp] IN keptIn ",
    "AND edge3.amount > $threshold ",
    "AND $startTime < edge3.timestamp AND edge3.timestamp < $endTime) AS admitted ",
    "WITH mid, keptOut, sum1, numEdge2, sum2, ",
    "sum(CASE WHEN admitted THEN edge3.amount END) AS sum3 ",
    "OPTIONAL MATCH (mid)-[edge4:transfer]->(down:Account) ",
    "WITH sum1, numEdge2, sum2, sum3, edge4, ([down.id, edge4.timestamp] IN keptOut ",
    "AND edge4.amount > $threshold ",
    "AND $startTime < edge4.timestamp AND edge4.timestamp < $endTime) AS admitted ",
    "WITH sum1, numEdge2, sum2, sum3, count(CASE WHEN admitted THEN 1 END) AS numEdge4, ",
    "sum(CASE WHEN admitted THEN edge4.amount END) AS sum4 ",
    "RETURN CASE WHEN numEdge2 = 0 THEN -1.0 ",
    "ELSE round(sum1 / sum2 * 1000) / 1000 END AS ratioRepay, ",
    "CASE WHEN numEdge4 = 0 THEN -1.0 ",
    "ELSE round(sum1 / sum4 * 1000) / 1000 END AS ratioDeposit, ",
    "CASE WHEN numEdge4 = 0 THEN -1.0 ",
    "ELSE round(sum3 / sum4 * 1000) / 1000 END AS ratioTransfer"
);

const TCR11: &str = concat!(
    "MATCH (person:Person {id: $id})-[:guarantee*0..]->(v:Person) ",
    "WITH DISTINCT person, v ",
    "OPTIONAL MATCH (v)-[g:guarantee]->(w:Person) ",
    "WITH person, v, g, w ORDER BY g.timestamp DESC, w.id ASC ",
    "WITH person, v, collect([v.id, w.id, g.timestamp]) AS keys ",
    "UNWIND keys[0..$truncationLimit] AS key ",
    "WITH person, collect(key) AS admissible ",
    "OPTIONAL MATCH p = (person)-[r:guarantee*1..]->(:Person) ",
    "WITH admissible, [n IN nodes(p) | n.id] AS ids, [e IN r | e.timestamp] AS ts ",
    "WITH admissible, ids, ts ",
    "WHERE size([i IN range(0, size(ts) - 1) WHERE NOT ([ids[i], ids[i + 1], ts[i]] IN admissible) ",
    "OR ts[i] <= $startTime OR ts[i] >= $endTime]) = 0 ",
    "UNWIND ids[1..] AS reachedId ",
    "WITH DISTINCT reachedId ",
    "MATCH (:Person {id: reachedId})-[:apply]->(loan:Loan) ",
    "WITH DISTINCT loan ",
    "RETURN round(sum(loan.loanAmount) * 1000) / 1000 AS sumLoanAmount, ",
    "count(loan) AS numLoans"
);

const TCR12: &str = concat!(
    "MATCH (person:Person {id: $id})-[:own]->(pAcc:Account) ",
    "MATCH (pAcc)-[t:transfer]->(x:Account) ",
    "WITH pAcc, t, x ORDER BY t.timestamp DESC, x.id ASC ",
    "WITH pAcc, collect([x.id, t.timestamp])[0..$truncationLimit] AS kept ",
    "MATCH (pAcc)-[edge2:transfer]->(compAcc:Account)<-[:own]-(:Company) ",
    "WHERE [compAcc.id, edge2.timestamp] IN kept ",
    "AND $startTime < edge2.timestamp AND edge2.timestamp < $endTime ",
    "WITH compAcc, sum(edge2.amount) AS total ",
    "RETURN compAcc.id AS compAccountId, round(total * 1000) / 1000 AS sumEdge2Amount ",
    "ORDER BY sumEdge2Amount DESC, compAccountId ASC"
);

const TSR1: &str = "MATCH (account:Account {id: $id}) \
RETURN account.createTime AS createTime, account.isBlocked AS isBlocked, account.type AS type";

const TSR2: &str = concat!(
    "MATCH (account:Account {id: $id}) ",
    "OPTIONAL MATCH (account)-[edge1:transfer]->(:Account) ",
    "WHERE $startTime < edge1.timestamp AND edge1.timestamp < $endTime ",
    "WITH account, sum(edge1.amount) AS sum1, ",
    "coalesce(max(edge1.amount), -1.0) AS max1, count(edge1) AS numEdge1 ",
    "OPTIONAL MATCH (account)<-[edge2:transfer]-(:Account) ",
    "WHERE $startTime < edge2.timestamp AND edge2.timestamp < $endTime ",
    "WITH sum1, max1, numEdge1, sum(edge2.amount) AS sum2, ",
    "coalesce(max(edge2.amount), -1.0) AS max2, count(edge2) AS numEdge2 ",
    "RETURN round(sum1 * 1000) / 1000 AS sumEdge1Amount, ",
    "round(max1 * 1000) / 1000 AS maxEdge1Amount, numEdge1, ",
    "round(sum2 * 1000) / 1000 AS sumEdge2Amount, ",
    "round(max2 * 1000) / 1000 AS maxEdge2Amount, numEdge2"
);

const TSR3: &str = concat!(
    "MATCH (dst:Account {id: $id}) ",
    "OPTIONAL MATCH (dst)<-[edge2:transfer]-(:Account) ",
    "WITH dst, count(edge2) AS numEdge2 ",
    "OPTIONAL MATCH (dst)<-[edge1:transfer]-(src:Account) ",
    "WHERE src.isBlocked = true AND edge1.amount > $threshold ",
    "AND $startTime < edge1.timestamp AND edge1.timestamp < $endTime ",
    "WITH numEdge2, count(edge1) AS numEdge1 ",
    "RETURN CASE WHEN numEdge2 = 0 THEN -1.0 ",
    "ELSE round(toFloat(numEdge1) / numEdge2 * 1000) / 1000 END AS blockRatio"
);

const TSR4: &str = concat!(
    "MATCH (src:Account {id: $id})-[edge:transfer]->(dst:Account) ",
    "WHERE edge.amount > $threshold ",
    "AND $startTime < edge.timestamp AND edge.timestamp < $endTime ",
    "WITH dst, count(edge) AS numEdges, sum(edge.amount) AS total ",
    "RETURN dst.id AS dstId, numEdges, round(total * 1000) / 1000 AS sumAmount ",
    "ORDER BY sumAmount DESC, dstId ASC"
);

const TSR5: &str = concat!(
    "MATCH (dst:Account {id: $id})<-[edge:transfer]-(src:Account) ",
    "WHERE edge.amount > $threshold ",
    "AND $startTime < edge.timestamp AND edge.timestamp < $endTime ",
    "WITH src, count(edge) AS numEdges, sum(edge.amount) AS total ",
    "RETURN src.id AS srcId, numEdges, round(total * 1000) / 1000 AS sumAmount ",
    "ORDER BY sumAmount DESC, srcId ASC"
);

const TSR6: &str = concat!(
    "MATCH (src:Account {id: $id})<-[edge1:transfer]-(mid:Account)-[edge2:transfer]->(dst:Account) ",
    "WHERE dst.isBlocked = true AND dst.id <> src.id ",
    "AND $startTime < edge1.timestamp AND edge1.timestamp < $endTime ",
    "AND $startTime < edge2.timestamp AND edge2.timestamp < $endTime ",
    "RETURN DISTINCT dst.id AS dstId ORDER BY dstId ASC"
);

use ColumnKind::{Bool, Float3, Int, IntList, Text};

/// Every runnable read, in operation order.
pub static QUERIES: &[QueryDefinition] = &[
    QueryDefinition {
        operation: Operation::Tcr1,
        cypher: TCR1,
        parameters: &[ID, START, END, LIMIT, ORDER],
        columns: &[
            c("otherId", Int),
            c("accountDistance", Int),
            c("mediumId", Int),
            c("mediumType", Text),
        ],
        validation: EXACT,
        truncation: truncation(&["transfer, out, every account on the trace"]),
        semantics: "Accounts reached from the start account by 1..3 transfers whose timestamps \
                    strictly ascend and lie in the window, signed in by a blocked medium through a \
                    signIn in the window. One row per distinct (account, trace length, medium); an \
                    account reached at several lengths appears at each.",
        reference_reading: Some(
            "GPStore reads accountDistance as the breadth-first distance at which it first \
             reaches an account, once per account and never for the start account; this \
             reference follows the spec: every trace length (1..3) at which a valid ascending \
             trace reaches the account, since the spec says an account may appear at several \
             distances.",
        ),
        workarounds: &[W_EDGE_KEYS, W_HOP_LIST],
    },
    QueryDefinition {
        operation: Operation::Tcr2,
        cypher: TCR2,
        parameters: &[ID, START, END, LIMIT, ORDER],
        columns: &[
            c("otherId", Int),
            c("sumLoanAmount", Float3),
            c("sumLoanBalance", Float3),
        ],
        validation: EXACT,
        truncation: truncation(&[
            "transfer, in, every account on the trace, expanding upstream from the owned account",
        ]),
        semantics: "Accounts with a 1..3 transfer trace into an account the person owns, timestamps \
                    strictly ascending from upstream to downstream and in the window; per account, \
                    the sums over distinct loans that deposited into it in the window.",
        reference_reading: Some(
            "GPStore reads the person's own edges as truncated too, and its trace helper \
             (BFSWithPathTsOrder) is not in the published source; this reference follows the \
             spec: own edges are not truncated, and every 1..3 transfer trace with strictly \
             ascending timestamps in the window counts.",
        ),
        workarounds: &[W_EDGE_KEYS, W_HOP_LIST],
    },
    QueryDefinition {
        operation: Operation::Tcr3,
        cypher: TCR3,
        parameters: &[
            p("id1", ParameterKind::Id),
            p("id2", ParameterKind::Id),
            START,
            END,
        ],
        columns: &[c("shortestPathLength", Int)],
        validation: EXACT,
        truncation: None,
        semantics: "Length of the shortest directed transfer path whose every edge is in the window, \
                    or -1. Unbounded length; the minimum over all edge-distinct paths equals the \
                    shortest path.",
        reference_reading: None,
        workarounds: &[W_SHORTEST_PATH],
    },
    QueryDefinition {
        operation: Operation::Tcr4,
        cypher: TCR4,
        parameters: &[
            p("id1", ParameterKind::Id),
            p("id2", ParameterKind::Id),
            START,
            END,
        ],
        columns: &[
            c("otherId", Int),
            c("numEdge2", Int),
            c("sumEdge2Amount", Float3),
            c("maxEdge2Amount", Float3),
            c("numEdge3", Int),
            c("sumEdge3Amount", Float3),
            c("maxEdge3Amount", Float3),
        ],
        validation: EXACT,
        truncation: None,
        semantics: "Empty unless src transferred to dst in the window. Otherwise every account that \
                    received from dst (edge3) and sent to src (edge2) in the window, with count, sum \
                    and max of each edge set.",
        reference_reading: None,
        workarounds: &[],
    },
    QueryDefinition {
        operation: Operation::Tcr5,
        cypher: TCR5,
        parameters: &[ID, START, END, LIMIT, ORDER],
        columns: &[c("path", IntList)],
        validation: EXACT,
        truncation: truncation(&["transfer, out, every account on the trace"]),
        semantics: "Distinct account-id sequences of 1..3 transfer traces from an account the person \
                    owns, timestamps strictly ascending and in the window, no account repeated. The \
                    specification orders by length only; ties are ordered by the id sequence.",
        reference_reading: Some(
            "GPStore reads the person's own edges as truncated too and keeps traces that \
             revisit an account; this reference follows the spec: own edges are not truncated, \
             and traces with a repeated account are dropped.",
        ),
        workarounds: &[W_EDGE_KEYS, W_HOP_LIST],
    },
    QueryDefinition {
        operation: Operation::Tcr6,
        cypher: TCR6,
        parameters: &[
            ID,
            p("threshold1", ParameterKind::Float),
            p("threshold2", ParameterKind::Float),
            START,
            END,
            LIMIT,
            ORDER,
        ],
        columns: &[
            c("midId", Int),
            c("sumEdge1Amount", Float3),
            c("sumEdge2Amount", Float3),
        ],
        validation: EXACT,
        truncation: truncation(&[
            "withdraw, in, the card account",
            "transfer, in, each mid account",
        ]),
        semantics: "For a card account (type ends with 'card'), each mid account that withdrew to it \
                    above threshold2 in the window and received more than 3 transfers above \
                    threshold1 in the window.",
        reference_reading: Some(
            "Galaxybase, GPStore and Ultipa read \"more than 3 transfer-ins\" as more than 3 \
             edges; this reference follows the spec the same way and counts transfer edges, not \
             distinct source accounts. Counting distinct sources would change 901 of the 905 SF1 \
             bindings. Two SF1 bindings (lines 262 and 686) also depend on truncating each \
             adjacency before the window and amount filters.",
        ),
        workarounds: &[W_EDGE_KEYS],
    },
    QueryDefinition {
        operation: Operation::Tcr7,
        cypher: TCR7,
        parameters: &[
            ID,
            p("threshold", ParameterKind::Float),
            START,
            END,
            LIMIT,
            ORDER,
        ],
        columns: &[c("numSrc", Int), c("numDst", Int), c("inOutRatio", Float3)],
        validation: NORMALIZED,
        truncation: truncation(&["transfer, in, the account", "transfer, out, the account"]),
        semantics: "Distinct senders and receivers of transfers above the threshold in the window, \
                    and the transfer-in over transfer-out amount ratio, -1 without a transfer-out.",
        reference_reading: None,
        workarounds: &[W_EDGE_KEYS, W_OPTIONAL_WHERE],
    },
    QueryDefinition {
        operation: Operation::Tcr8,
        cypher: TCR8,
        parameters: &[
            ID,
            p("threshold", ParameterKind::Float),
            START,
            END,
            LIMIT,
            ORDER,
        ],
        columns: &[
            c("dstId", Int),
            c("ratio", Float3),
            c("minDistanceFromLoan", Int),
        ],
        validation: EXACT,
        truncation: truncation(&[
            "transfer and withdraw together, out, every account on the trace",
        ]),
        semantics: "From each account the loan deposited into in the window, 1..3 transfer or \
                    withdraw hops in the window, each with amount > threshold x upstream, where \
                    upstream is the summed in-window transfer-ins of the hop's source account. Per \
                    reached account: the distinct final-hop amounts over the loan amount, and 1 + the \
                    fewest hops.",
        reference_reading: Some(
            "GPStore reads inflow as every in-window edge from an expanded account into the \
             destination, including edges at or below the threshold, and expands each account \
             once; this reference follows the spec: inflow is the distinct final hops that pass \
             amount > threshold x upstream, with upstream the source account's summed in-window \
             transfer-ins (the upstream GPStore also uses). The spec does not define inflow \
             further.",
        ),
        workarounds: &[W_EDGE_KEYS, W_HOP_LIST, W_MULTI_TYPE],
    },
    QueryDefinition {
        operation: Operation::Tcr9,
        cypher: TCR9,
        parameters: &[
            ID,
            p("threshold", ParameterKind::Float),
            START,
            END,
            LIMIT,
            ORDER,
        ],
        columns: &[
            c("ratioRepay", Float3),
            c("ratioDeposit", Float3),
            c("ratioTransfer", Float3),
        ],
        validation: NORMALIZED,
        truncation: truncation(&[
            "transfer, in, the account (edge3)",
            "transfer, out, the account (edge4)",
        ]),
        semantics: "Deposit (edge1), repay (edge2), transfer-in (edge3) and transfer-out (edge4) \
                    sums above the threshold in the window; edge1/edge2, edge1/edge4 and \
                    edge3/edge4, -1 when the divisor has no edge.",
        reference_reading: Some(
            "GPStore reads the deposit (edge1) and repay (edge2) expansions as truncated too; \
             this reference follows the spec, which does not name the truncated steps, by \
             truncating only the transfer expansions (edge3, edge4), as Galaxybase does.",
        ),
        workarounds: &[W_EDGE_KEYS, W_OPTIONAL_WHERE],
    },
    QueryDefinition {
        operation: Operation::Tcr10,
        cypher: crate::LIVE_TCR10_QUERY,
        parameters: &[
            p("pid1", ParameterKind::Id),
            p("pid2", ParameterKind::Id),
            START,
            END,
        ],
        columns: &[c("jaccardSimilarity", Float3)],
        validation: NORMALIZED,
        truncation: None,
        semantics: "Jaccard similarity of the companies each person invested in during the window; \
                    0 when neither invested.",
        reference_reading: None,
        workarounds: &[],
    },
    QueryDefinition {
        operation: Operation::Tcr11,
        cypher: TCR11,
        parameters: &[ID, START, END, LIMIT, ORDER],
        columns: &[c("sumLoanAmount", Float3), c("numLoans", Int)],
        validation: EXACT,
        truncation: truncation(&["guarantee, out, every person on the chain"]),
        semantics: "Every person reached by a guarantee chain of any length, each guarantee in the \
                    window; the summed amount and count of the distinct loans they applied for.",
        reference_reading: Some(
            "GPStore reads each reached person's apply edges as truncated too; this \
             reference follows the spec: guarantee chains of any length (\"until end\", as \
             GPStore also reads it), with only the guarantee steps truncated and every applied \
             loan counted. Galaxybase, TuGraph and Ultipa stop at 5 hops, which changes 27 of the \
             983 SF1 bindings; that is the reference's only disagreement with a third-party \
             result.",
        ),
        workarounds: &[W_EDGE_KEYS, W_HOP_LIST],
    },
    QueryDefinition {
        operation: Operation::Tcr12,
        cypher: TCR12,
        parameters: &[ID, START, END, LIMIT, ORDER],
        columns: &[c("compAccountId", Int), c("sumEdge2Amount", Float3)],
        validation: EXACT,
        truncation: truncation(&["transfer, out, each account the person owns"]),
        semantics: "Company-owned accounts the person's accounts transferred to in the window, with \
                    the transfer sum.",
        reference_reading: None,
        workarounds: &[W_EDGE_KEYS],
    },
    QueryDefinition {
        operation: Operation::Tsr1,
        cypher: TSR1,
        parameters: &[ID],
        columns: &[c("createTime", Int), c("isBlocked", Bool), c("type", Text)],
        validation: EXACT,
        truncation: None,
        semantics: "The account's createTime, isBlocked and type.",
        reference_reading: None,
        workarounds: &[],
    },
    QueryDefinition {
        operation: Operation::Tsr2,
        cypher: TSR2,
        parameters: &[ID, START, END],
        columns: &[
            c("sumEdge1Amount", Float3),
            c("maxEdge1Amount", Float3),
            c("numEdge1", Int),
            c("sumEdge2Amount", Float3),
            c("maxEdge2Amount", Float3),
            c("numEdge2", Int),
        ],
        validation: EXACT,
        truncation: None,
        semantics: "Sum, max (-1 if none) and count of the account's transfer-outs (edge1) and \
                    transfer-ins (edge2) in the window.",
        reference_reading: None,
        workarounds: &[],
    },
    QueryDefinition {
        operation: Operation::Tsr3,
        cypher: TSR3,
        parameters: &[ID, p("threshold", ParameterKind::Float), START, END],
        columns: &[c("blockRatio", Float3)],
        validation: EXACT,
        truncation: None,
        semantics: "Transfer-ins from blocked accounts above the threshold in the window (edge1) over \
                    all transfer-ins (edge2, unfiltered as in the specification's pattern), -1 when \
                    there is no transfer-in.",
        reference_reading: None,
        workarounds: &[],
    },
    QueryDefinition {
        operation: Operation::Tsr4,
        cypher: TSR4,
        parameters: &[ID, p("threshold", ParameterKind::Float), START, END],
        columns: &[c("dstId", Int), c("numEdges", Int), c("sumAmount", Float3)],
        validation: EXACT,
        truncation: None,
        semantics: "Per destination, count and sum of the account's transfer-outs above the \
                    threshold in the window.",
        reference_reading: None,
        workarounds: &[],
    },
    QueryDefinition {
        operation: Operation::Tsr5,
        cypher: TSR5,
        parameters: &[ID, p("threshold", ParameterKind::Float), START, END],
        columns: &[c("srcId", Int), c("numEdges", Int), c("sumAmount", Float3)],
        validation: EXACT,
        truncation: None,
        semantics: "Per source, count and sum of the account's transfer-ins above the threshold in \
                    the window.",
        reference_reading: None,
        workarounds: &[],
    },
    QueryDefinition {
        operation: Operation::Tsr6,
        cypher: TSR6,
        parameters: &[ID, START, END],
        columns: &[c("dstId", Int)],
        validation: EXACT,
        truncation: None,
        semantics: "Blocked accounts, other than the given one, that received a transfer in the \
                    window from an account that also transferred to the given one in the window.",
        reference_reading: None,
        workarounds: &[],
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn binding(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        value.as_object().cloned().expect("binding object")
    }

    fn cypher_parameters(cypher: &str) -> BTreeSet<String> {
        cypher
            .split('$')
            .skip(1)
            .map(|rest| {
                rest.chars()
                    .take_while(char::is_ascii_alphanumeric)
                    .collect::<String>()
            })
            .collect()
    }

    #[test]
    fn catalog_lists_each_read_once_in_operation_order() {
        let catalog = query_catalog();
        assert_eq!(catalog.schema, QUERY_CATALOG_SCHEMA);
        let operations: Vec<Operation> = catalog.queries.iter().map(|q| q.operation).collect();
        let mut sorted = operations.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(operations, sorted);
        assert_eq!(operations.len(), 18);
        let json = serde_json::to_value(&catalog).unwrap();
        assert_eq!(json["queries"][0]["operation"], "TCR1");
        assert_eq!(json["queries"][0]["truncation"]["default_limit"], 500);
        assert_eq!(
            json["queries"][0]["truncation"]["order"],
            "TIMESTAMP_DESCENDING"
        );
        assert_eq!(json["queries"][0]["parameters"][0]["kind"], "id");
        assert_eq!(json["queries"][0]["columns"][3]["kind"], "text");
    }

    #[test]
    fn truncation_is_declared_exactly_where_the_specification_asks_for_it() {
        let truncated: BTreeSet<&str> = QUERIES
            .iter()
            .filter(|query| query.truncation.is_some())
            .map(|query| query.operation.code())
            .collect();
        let expected: BTreeSet<&str> = [
            "TCR1", "TCR2", "TCR5", "TCR6", "TCR7", "TCR8", "TCR9", "TCR11", "TCR12",
        ]
        .into_iter()
        .collect();
        assert_eq!(truncated, expected);
        for query in QUERIES {
            let names: BTreeSet<&str> = query.parameters.iter().map(|p| p.name).collect();
            let has_limit = names.contains("truncationLimit");
            assert_eq!(has_limit, query.truncation.is_some(), "{}", query.operation);
            assert_eq!(
                names.contains("truncationOrder"),
                has_limit,
                "{}",
                query.operation
            );
            if let Some(truncation) = query.truncation {
                assert_eq!(truncation.default_limit, 500);
                assert_eq!(truncation.order, TRUNCATION_ORDER);
                assert!(!truncation.truncated_steps.is_empty());
                assert!(query.cypher.contains("[0..$truncationLimit]"));
                assert!(query.cypher.contains("timestamp DESC"));
            }
        }
    }

    #[test]
    fn the_tie_break_is_labelled_a_variance_on_every_truncated_read() {
        for query in QUERIES {
            if let Some(truncation) = query.truncation {
                assert_eq!(truncation.tie_break_variance, TIE_BREAK_VARIANCE);
                assert!(
                    truncation
                        .tie_break_variance
                        .starts_with("variance from the specification")
                );
            }
        }
        let json = serde_json::to_value(query_catalog()).unwrap();
        assert!(
            json["queries"][0]["truncation"]["tie_break_variance"]
                .as_str()
                .unwrap()
                .contains("far-endpoint id ascending")
        );
    }

    #[test]
    fn gpstore_readings_are_informational_and_the_reference_follows_the_spec() {
        let noted: BTreeSet<&str> = QUERIES
            .iter()
            .filter(|query| query.reference_reading.is_some())
            .map(|query| query.operation.code())
            .collect();
        let expected: BTreeSet<&str> = ["TCR1", "TCR2", "TCR5", "TCR6", "TCR8", "TCR9", "TCR11"]
            .into_iter()
            .collect();
        assert_eq!(noted, expected);
        for query in QUERIES {
            if let Some(reading) = query.reference_reading {
                assert!(
                    reading.starts_with("GPStore reads")
                        || reading.starts_with("Galaxybase, GPStore and Ultipa read"),
                    "{}",
                    query.operation
                );
                assert!(
                    reading.contains("this reference follows the spec"),
                    "{}",
                    query.operation
                );
                assert!(!reading.contains("scorecard"), "{}", query.operation);
                assert!(!reading.contains("may change"), "{}", query.operation);
            }
        }
    }

    #[test]
    fn every_workaround_in_the_cypher_cites_its_defect() {
        for query in QUERIES {
            for workaround in query.workarounds {
                assert!(
                    workaround.contains("#1888"),
                    "{}: {workaround}",
                    query.operation
                );
                assert!(!workaround.contains("#1887"), "{}", query.operation);
            }
            let cites = |text: &str| query.workarounds.iter().any(|w| w.contains(text));
            if query.cypher.contains("] IN ") {
                assert!(
                    cites("tie-break variance"),
                    "{} compares edge keys",
                    query.operation
                );
            }
            if query.cypher.contains("WHERE NOT ([ids[i]") {
                assert!(cites("reduce()"), "{} checks hops by list", query.operation);
            }
            if query.cypher.contains("type(e) IN") {
                assert!(cites("[:transfer|withdraw*1..3]"), "{}", query.operation);
            }
            if query.cypher.contains("CASE WHEN admitted") {
                assert!(cites("#1888 D7"), "{}", query.operation);
            }
        }
        let tcr3 = query_definition(Operation::Tcr3).unwrap();
        assert!(tcr3.workarounds.iter().any(|w| w.contains("shortestPath")));
    }

    #[test]
    fn every_cypher_parameter_is_declared_and_every_declared_one_is_used() {
        for query in QUERIES {
            let used = cypher_parameters(query.cypher);
            let declared: BTreeSet<String> = query
                .parameters
                .iter()
                .filter(|p| p.kind != ParameterKind::TruncationOrder)
                .map(|p| p.name.to_string())
                .collect();
            assert_eq!(used, declared, "{}", query.operation);
        }
    }

    #[test]
    fn every_declared_column_is_returned_by_name() {
        for query in QUERIES {
            let returned = query
                .cypher
                .rsplit_once("RETURN ")
                .map(|(_, tail)| tail)
                .unwrap();
            for column in query.columns {
                assert!(
                    returned.contains(column.name),
                    "{} does not return {}",
                    query.operation,
                    column.name
                );
            }
        }
    }

    #[test]
    fn bind_accepts_a_complete_binding_and_drops_the_order() {
        let tcr6 = query_definition(Operation::Tcr6).unwrap();
        let params = tcr6
            .bind(&binding(serde_json::json!({
                "id": 150, "threshold1": 100, "threshold2": 100.5,
                "startTime": 1000, "endTime": 2000,
                "truncationLimit": 500, "truncationOrder": "TIMESTAMP_DESCENDING"
            })))
            .unwrap();
        assert_eq!(params.len(), 6);
        assert_eq!(params["threshold1"], IrLiteral::Float(100.0));
        assert_eq!(params["threshold2"], IrLiteral::Float(100.5));
        assert_eq!(params["truncationLimit"], IrLiteral::Int(500));
        assert!(!params.contains_key("truncationOrder"));
    }

    #[test]
    fn bind_refuses_other_truncation_orders_with_a_typed_cause() {
        let tcr1 = query_definition(Operation::Tcr1).unwrap();
        for order in [
            "TIMESTAMP_ASCENDING",
            "AMOUNT_DESCENDING",
            "AMOUNT_ASCENDING",
        ] {
            let error = tcr1
                .bind(&binding(serde_json::json!({
                    "id": 1, "startTime": 0, "endTime": 1,
                    "truncationLimit": 500, "truncationOrder": order
                })))
                .unwrap_err();
            assert!(
                matches!(&error, SuiteError::SemanticIncompatibility { cause, .. } if cause == TRUNCATION_ORDER_CAUSE),
                "{error}"
            );
        }
    }

    #[test]
    fn bind_rejects_missing_extra_mistyped_and_non_positive_parameters() {
        let tcr12 = query_definition(Operation::Tcr12).unwrap();
        let complete = serde_json::json!({
            "id": 6, "startTime": 1000, "endTime": 2000,
            "truncationLimit": 2, "truncationOrder": "TIMESTAMP_DESCENDING"
        });
        tcr12.bind(&binding(complete.clone())).unwrap();
        let mut missing = complete.clone();
        missing.as_object_mut().unwrap().remove("truncationLimit");
        let mut extra = complete.clone();
        extra["threshold"] = serde_json::json!(1.0);
        let mut mistyped = complete.clone();
        mistyped["id"] = serde_json::json!("6");
        let mut zero = complete;
        zero["truncationLimit"] = serde_json::json!(0);
        for (label, value) in [
            ("missing", missing),
            ("extra", extra),
            ("mistyped", mistyped),
            ("zero limit", zero),
        ] {
            let error = tcr12.bind(&binding(value)).unwrap_err();
            assert!(
                matches!(error, SuiteError::InvalidDocument(_)),
                "{label}: {error}"
            );
        }
    }
}
