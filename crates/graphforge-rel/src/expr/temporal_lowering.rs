//! Temporal constructor, projection, truncation, and epoch lowering.
//!
//! Lowering state and dispatch remain in the parent; runtime semantics use existing adapters.

use super::{
    CYPHER_DATE_PROJECT, CYPHER_DATE_TRUNCATE, CYPHER_DATETIME_PROJECT, CYPHER_DATETIME_TRUNCATE,
    CYPHER_DURATION_BETWEEN, CYPHER_DURATION_PARSE, CYPHER_LOCALDATETIME_PROJECT,
    CYPHER_LOCALDATETIME_TRUNCATE, CYPHER_LOCALTIME_PROJECT, CYPHER_LOCALTIME_TRUNCATE,
    CYPHER_TIME_PROJECT, CYPHER_TIME_TRUNCATE, DfExpr, ExprId, ExprLowerer, IrExpr, IrLiteral,
    LoweringError, ScalarValue, date_scalar, datetime_scalar, duration_scalar, lit,
    localdatetime_scalar, render_temporal, resolve_builtin, temporal_null_scalar, time_scalar,
};

impl ExprLowerer<'_> {
    /// Lower a temporal constructor — `date`/`localtime`/`time`/
    /// `localdatetime`/`datetime`/`duration`. When the single argument is a
    /// constant the openCypher TCK uses — an ISO string literal or a map of
    /// literal fields — parse and canonicalise it at lowering time and emit the
    /// quoted-ISO `Utf8` literal the TCK renders, so no runtime UDF is needed.
    /// The supported forms are documented in [`crate::temporal`]. Non-constant /
    /// unsupported-form arguments fall back to `resolve_builtin` (today only
    /// `date` has a runtime path, via `to_date`/`to_char`; the others error).
    /// (#599)
    /// Whether the call has a single argument that is a literal `null` — a
    /// temporal constructor / clock function of `null` propagates to `null`
    /// (openCypher Temporal4 [13]). (#920)
    pub(super) fn sole_arg_is_null(&self, args: &[ExprId]) -> bool {
        matches!(args, [a] if matches!(self.arena.get(*a), IrExpr::Literal(graphforge_ir::IrLiteral::Null)))
    }

    /// Fold a zero-arg current-time constructor (`date()`/`localtime()`/`time()`/
    /// `localdatetime()`/`datetime()`) to a constant scalar from a single `now`
    /// captured once per lowering, so two calls in one query fold IDENTICALLY
    /// (`duration.inSeconds(localtime(), localtime())` → `PT0S`). `time`/`datetime`
    /// use UTC (offset 0). Non-deterministic by nature — a deliberate exception to
    /// the engine's constant-folding determinism; only the difference-invariant is
    /// exercised by the TCK. (#1007, Temporal10 [12])
    fn lower_clock_now(&self, name: &str) -> DfExpr {
        use chrono::Timelike;
        let now = *self.now.get_or_init(|| chrono::Utc::now().naive_utc());
        let days = crate::temporal::date_to_epoch_days(now.date()).unwrap_or(0);
        let nanos = i64::from(now.time().num_seconds_from_midnight()) * 1_000_000_000
            + i64::from(now.time().nanosecond());
        let scalar = match name {
            "date" => date_scalar(Some(days)),
            "localtime" => ScalarValue::Time64Nanosecond(Some(nanos)),
            "localdatetime" => localdatetime_scalar(Some((days, nanos))),
            "time" => time_scalar(Some((nanos, 0))),
            // "datetime": UTC instant, offset 0, no named zone.
            _ => datetime_scalar(Some((days, nanos, 0, None))),
        };
        lit(scalar)
    }

    pub(super) fn lower_temporal(
        &self,
        name: &str,
        args: &[ExprId],
    ) -> Result<DfExpr, LoweringError> {
        // A temporal constructor of a literal `null` is `null`, typed so the
        // temporal Arrow contract survives null propagation (#920).
        if self.sole_arg_is_null(args) {
            return Ok(DfExpr::Literal(temporal_null_scalar(name), None));
        }
        // A zero-arg current-time constructor `date()`/`localtime()`/`time()`/
        // `localdatetime()`/`datetime()` (#1007). `duration()` has no clock form.
        if args.is_empty() && name != "duration" {
            return Ok(self.lower_clock_now(name));
        }
        if let [arg] = args {
            // `date` is a typed `Struct{epoch_day: Int64}` value (ADR 0009/0012). A
            // constant lowers to a date-struct scalar; a runtime argument (a column,
            // or `{date: other, …overrides}`) goes through the `cypher_date_project`
            // UDF (#920).
            if name == "date" {
                if let Some(days) = self.const_date(*arg) {
                    return Ok(DfExpr::Literal(date_scalar(Some(days)), None));
                }
                // `date(other)` / `date({date: other, …})` → projection UDF. A
                // map *without* a `date` anchor (runtime field construction)
                // returns None and falls through to the runtime builtin path.
                if let Some(projected) = self.lower_date_runtime(*arg)? {
                    return Ok(projected);
                }
            }
            // `localtime` is a typed `Time64(Nanosecond)` value (ADR 0009). A
            // constant lowers to a scalar; a runtime argument (a column, or
            // `{time: other, …overrides}`) goes through `cypher_localtime_project`.
            if name == "localtime" {
                if let Some(nanos) = self.const_local_time(*arg) {
                    return Ok(DfExpr::Literal(
                        ScalarValue::Time64Nanosecond(Some(nanos)),
                        None,
                    ));
                }
                if let Some(projected) = self.lower_localtime_runtime(*arg)? {
                    return Ok(projected);
                }
            }
            // `localdatetime` is a typed `Struct{date: Date32, time: Time64(ns)}`
            // value (ADR 0009) — a date + time-of-day with no zone, two-field so
            // it spans the full year range at nanosecond precision. A constant
            // lowers to a struct scalar; a runtime argument goes through
            // `cypher_localdatetime_project`.
            if name == "localdatetime" {
                if let Some((days, nanos)) = self.const_local_date_time(*arg) {
                    return Ok(DfExpr::Literal(
                        localdatetime_scalar(Some((days, nanos))),
                        None,
                    ));
                }
                if let Some(projected) = self.lower_localdatetime_runtime(*arg)? {
                    return Ok(projected);
                }
            }
            // `time` is a typed `Struct{time: Time64(ns), offset: Int32}` value
            // (ADR 0009) — a time of day with a zone offset. A constant lowers to
            // a struct scalar; a runtime argument goes through `cypher_time_project`.
            if name == "time" {
                if let Some((nanos, offset)) = self.const_time(*arg) {
                    return Ok(DfExpr::Literal(time_scalar(Some((nanos, offset))), None));
                }
                if let Some(projected) = self.lower_time_runtime(*arg)? {
                    return Ok(projected);
                }
            }
            // `datetime` is a typed `Struct{date: Date32, time: Time64(ns),
            // offset: Int32, zone: Utf8?}` value (ADR 0009) — a date + time + zone
            // (resolved offset plus an optional named-IANA-zone label). A constant
            // lowers to a struct scalar; a runtime argument goes through
            // `cypher_datetime_project`.
            if name == "datetime" {
                if let Some(parts) = self.const_datetime(*arg) {
                    return Ok(DfExpr::Literal(datetime_scalar(Some(parts)), None));
                }
                if let Some(projected) = self.lower_datetime_runtime(*arg)? {
                    return Ok(projected);
                }
            }
            // `duration` is a typed `Struct{months, days, seconds, nanos}` value (ADR
            // 0009). A constant (literal ISO string or field map) lowers to a
            // struct scalar; a runtime ISO-string argument (e.g.
            // `duration(toString(d))`) goes through `cypher_duration_parse`.
            if name == "duration" {
                if let Some(dur) = self.const_duration(*arg) {
                    return Ok(DfExpr::Literal(duration_scalar(Some(dur)), None));
                }
                let lowered = self.lower(*arg)?;
                if self.is_string_typed(&lowered) {
                    return Ok(CYPHER_DURATION_PARSE.call(vec![lowered]));
                }
            }
            match self.arena.get(*arg) {
                IrExpr::Literal(IrLiteral::Str(s)) => {
                    if let Some(rendered) = render_temporal(name, s) {
                        return Ok(lit(rendered));
                    }
                }
                IrExpr::MapLiteral(entries) => {
                    if let Some(fields) = self.extract_temporal_fields(entries)
                        && let Some(rendered) = crate::temporal::render_temporal_map(name, &fields)
                    {
                        return Ok(lit(rendered));
                    }
                }
                _ => {}
            }
        }
        let lowered: Vec<DfExpr> = args
            .iter()
            .map(|&a| self.lower(a))
            .collect::<Result<_, _>>()?;
        resolve_builtin(name, lowered, {
            let hydration = if name.eq_ignore_ascii_case("_path_nodes") {
                self.path_node_hydration()?
            } else {
                None
            };
            || hydration
        })
        .ok_or_else(|| LoweringError::UnknownFunction(name.to_string()))
    }

    /// Resolve a `date(<arg>)` argument to constant i64 epoch-days when the
    /// argument is a literal ISO string or a literal field map. (ADR 0009/0012)
    fn const_date(&self, arg: ExprId) -> Option<i64> {
        match self.arena.get(arg) {
            IrExpr::Literal(IrLiteral::Str(s)) => crate::temporal::parse_date_string(s),
            IrExpr::MapLiteral(entries) => {
                let fields = self.extract_temporal_fields(entries)?;
                crate::temporal::date_from_map(&fields)
            }
            _ => None,
        }
    }

    /// Lower a runtime (non-constant) `date(<arg>)` to a `cypher_date_project`
    /// call returning `Date32`: `date({date: base, …overrides})` projects the
    /// base date's components; a bare `date(<expr>)` extracts the date from a
    /// `Date32` or ISO date/datetime string (no overrides). Returns `None` for a
    /// map *without* a `date` anchor (runtime field construction — not a
    /// projection), so the caller falls through to the runtime builtin. (#920)
    fn lower_date_runtime(&self, arg: ExprId) -> Result<Option<DfExpr>, LoweringError> {
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let (base, overrides) = match self.arena.get(arg) {
            IrExpr::MapLiteral(entries) if entries.iter().any(|(k, _)| k == "date") => {
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let base = self.lower(field("date").expect("checked `date` key exists"))?;
                let ov = |name: &str| match field(name) {
                    Some(id) => self.lower(id),
                    None => Ok(null_i64()),
                };
                let overrides = [
                    ov("year")?,
                    ov("month")?,
                    ov("day")?,
                    ov("week")?,
                    ov("dayOfWeek")?,
                    ov("ordinalDay")?,
                    ov("quarter")?,
                    ov("dayOfQuarter")?,
                ];
                (base, overrides)
            }
            // A map without a `date` anchor is field-construction, not a
            // projection — let the caller's runtime path handle it.
            IrExpr::MapLiteral(_) => return Ok(None),
            // A bare `date(<expr>)` — extract the date, no overrides.
            _ => (self.lower(arg)?, std::array::from_fn(|_| null_i64())),
        };
        let mut call_args = Vec::with_capacity(9);
        call_args.push(base);
        call_args.extend(overrides);
        Ok(Some(CYPHER_DATE_PROJECT.call(call_args)))
    }

    /// Resolve a `localtime(<arg>)` argument to constant nanoseconds-of-day when
    /// the argument is a literal ISO string or a literal field map. (ADR 0009)
    fn const_local_time(&self, arg: ExprId) -> Option<i64> {
        match self.arena.get(arg) {
            IrExpr::Literal(IrLiteral::Str(s)) => crate::temporal::localtime_nanos_from_str(s),
            IrExpr::MapLiteral(entries) => {
                let fields = self.extract_temporal_fields(entries)?;
                crate::temporal::localtime_nanos_from_map(&fields)
            }
            _ => None,
        }
    }

    /// Lower a runtime `localtime(<arg>)` to a `cypher_localtime_project` call
    /// returning `Time64(Nanosecond)`: `localtime({time: base, …overrides})`
    /// projects the base's time-of-day; a bare `localtime(<expr>)` extracts the
    /// time-of-day from a `Time64` or any ISO temporal string. Returns `None` for
    /// a map *without* a `time` anchor (field-construction, not projection), so
    /// the caller falls through to the runtime builtin. (ADR 0009)
    fn lower_localtime_runtime(&self, arg: ExprId) -> Result<Option<DfExpr>, LoweringError> {
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let (base, overrides) = match self.arena.get(arg) {
            IrExpr::MapLiteral(entries) if entries.iter().any(|(k, _)| k == "time") => {
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let base = self.lower(field("time").expect("checked `time` key exists"))?;
                let ov = |name: &str| match field(name) {
                    Some(id) => self.lower(id),
                    None => Ok(null_i64()),
                };
                let overrides = [
                    ov("hour")?,
                    ov("minute")?,
                    ov("second")?,
                    ov("millisecond")?,
                    ov("microsecond")?,
                    ov("nanosecond")?,
                ];
                (base, overrides)
            }
            IrExpr::MapLiteral(_) => return Ok(None),
            _ => (self.lower(arg)?, std::array::from_fn(|_| null_i64())),
        };
        let mut call_args = Vec::with_capacity(7);
        call_args.push(base);
        call_args.extend(overrides);
        Ok(Some(CYPHER_LOCALTIME_PROJECT.call(call_args)))
    }

    /// Resolve a `localdatetime(<arg>)` argument to constant `(date_days,
    /// nanoseconds_of_day)` when the argument is a literal ISO string or a literal
    /// field map. (ADR 0009)
    fn const_local_date_time(&self, arg: ExprId) -> Option<(i64, i64)> {
        match self.arena.get(arg) {
            IrExpr::Literal(IrLiteral::Str(s)) => crate::temporal::localdatetime_parts_from_str(s),
            IrExpr::MapLiteral(entries) => {
                let fields = self.extract_temporal_fields(entries)?;
                crate::temporal::localdatetime_parts_from_map(&fields)
            }
            _ => None,
        }
    }

    /// Lower a runtime `localdatetime(<arg>)` to a `cypher_localdatetime_project`
    /// call returning `Timestamp(Nanosecond, None)`. The map's `datetime`/`date`
    /// anchor (or a bare `localdatetime(<expr>)`) supplies the base date and the
    /// `datetime`/`time` anchor the base time; the remaining fields are date and
    /// time overrides (a missing date defaults to the epoch, a missing time to
    /// midnight, so explicit fields act as construction defaults). (ADR 0009)
    fn lower_localdatetime_runtime(&self, arg: ExprId) -> Result<Option<DfExpr>, LoweringError> {
        let null = || DfExpr::Literal(ScalarValue::Null, None);
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let (date_src, time_src, overrides) =
            if let IrExpr::MapLiteral(entries) = self.arena.get(arg) {
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let lower_or = |id: Option<ExprId>, default: &dyn Fn() -> DfExpr| match id {
                    Some(id) => self.lower(id),
                    None => Ok(default()),
                };
                // A `datetime:` anchor supplies BOTH base date and base time; a
                // `date:`/`time:` anchor supplies one each.
                let date_anchor = field("datetime").or_else(|| field("date"));
                let time_anchor = field("datetime").or_else(|| field("time"));
                let date_src = lower_or(date_anchor, &null)?;
                let time_src = lower_or(time_anchor, &null)?;
                let ov = |name: &str| lower_or(field(name), &null_i64);
                let overrides = [
                    ov("year")?,
                    ov("month")?,
                    ov("day")?,
                    ov("week")?,
                    ov("dayOfWeek")?,
                    ov("ordinalDay")?,
                    ov("quarter")?,
                    ov("dayOfQuarter")?,
                    ov("hour")?,
                    ov("minute")?,
                    ov("second")?,
                    ov("millisecond")?,
                    ov("microsecond")?,
                    ov("nanosecond")?,
                ];
                (date_src, time_src, overrides)
            } else {
                // A bare `localdatetime(<expr>)` — the value is both date and time
                // source, no overrides.
                let base = self.lower(arg)?;
                (base.clone(), base, std::array::from_fn(|_| null_i64()))
            };
        let mut call_args = Vec::with_capacity(16);
        call_args.push(date_src);
        call_args.push(time_src);
        call_args.extend(overrides);
        Ok(Some(CYPHER_LOCALDATETIME_PROJECT.call(call_args)))
    }

    /// Resolve a `time(<arg>)` argument to constant `(nanoseconds_of_day,
    /// offset_seconds)` when the argument is a literal ISO string or a literal
    /// field map. (ADR 0009)
    fn const_time(&self, arg: ExprId) -> Option<(i64, i32)> {
        match self.arena.get(arg) {
            IrExpr::Literal(IrLiteral::Str(s)) => crate::temporal::time_value_from_str(s),
            IrExpr::MapLiteral(entries) => {
                let fields = self.extract_temporal_fields(entries)?;
                crate::temporal::time_value_from_map(&fields)
            }
            _ => None,
        }
    }

    /// Lower a runtime `time(<arg>)` to a `cypher_time_project` call returning the
    /// `time` struct: `time({time: base, …overrides, timezone})` / a bare
    /// `time(<expr>)`. The base's time-of-day comes from a `Time64`/`time`-struct/
    /// `localdatetime`-struct/temporal-string; component overrides and the zone
    /// (`timezone`) apply on top. Returns `None` for a map without a `time` anchor
    /// (field-construction, not projection). (ADR 0009)
    fn lower_time_runtime(&self, arg: ExprId) -> Result<Option<DfExpr>, LoweringError> {
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let null_str = || DfExpr::Literal(ScalarValue::Utf8(None), None);
        let (base, overrides, timezone) = match self.arena.get(arg) {
            IrExpr::MapLiteral(entries) if entries.iter().any(|(k, _)| k == "time") => {
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let base = self.lower(field("time").expect("checked `time` key exists"))?;
                let ov = |name: &str| match field(name) {
                    Some(id) => self.lower(id),
                    None => Ok(null_i64()),
                };
                let overrides = [
                    ov("hour")?,
                    ov("minute")?,
                    ov("second")?,
                    ov("millisecond")?,
                    ov("microsecond")?,
                    ov("nanosecond")?,
                ];
                let timezone = match field("timezone") {
                    Some(id) => self.lower(id)?,
                    None => null_str(),
                };
                (base, overrides, timezone)
            }
            // A map without a `time` anchor is field-construction, not
            // projection; and a string literal that `const_time` rejected (e.g.
            // no offset) must NOT be leniently re-parsed by the UDF —
            // `time('21:40')` is an error, not `21:40Z`. Both fall through so the
            // untyped path handles them.
            IrExpr::MapLiteral(_) | IrExpr::Literal(IrLiteral::Str(_)) => return Ok(None),
            _ => (
                self.lower(arg)?,
                std::array::from_fn(|_| null_i64()),
                null_str(),
            ),
        };
        let mut call_args = Vec::with_capacity(8);
        call_args.push(base);
        call_args.extend(overrides);
        call_args.push(timezone);
        Ok(Some(CYPHER_TIME_PROJECT.call(call_args)))
    }

    /// Resolve a `datetime(<arg>)` argument to constant `(date_days, nanos,
    /// offset_seconds, zone_label)` when the argument is a literal ISO string or a
    /// literal field map. (ADR 0009)
    fn const_datetime(&self, arg: ExprId) -> Option<(i64, i64, i32, Option<String>)> {
        match self.arena.get(arg) {
            IrExpr::Literal(IrLiteral::Str(s)) => crate::temporal::datetime_value_from_str(s),
            IrExpr::MapLiteral(entries) => {
                let fields = self.extract_temporal_fields(entries)?;
                crate::temporal::datetime_value_from_map(&fields)
            }
            _ => None,
        }
    }

    /// Resolve a `duration(<arg>)` argument to a constant [`DurationValue`] when
    /// the argument is a literal ISO string or a literal field map. (#920)
    fn const_duration(&self, arg: ExprId) -> Option<crate::temporal::DurationValue> {
        match self.arena.get(arg) {
            IrExpr::Literal(IrLiteral::Str(s)) => crate::temporal::duration_value_from_str(s),
            IrExpr::MapLiteral(entries) => {
                let fields = self.extract_temporal_fields(entries)?;
                crate::temporal::duration_value_from_map(&fields)
            }
            _ => None,
        }
    }

    /// Lower a runtime `datetime(<arg>)` to a `cypher_datetime_project` call
    /// returning the `datetime` struct. The map's `datetime`/`date` anchor (or a
    /// bare `datetime(<expr>)`) supplies the base date, the `datetime`/`time`
    /// anchor the base time (and its offset/zone); the remaining fields are date
    /// and time overrides, and `timezone` re-zones the result. Returns `None` for
    /// a string literal (`const_datetime` already validated it). (ADR 0009)
    fn lower_datetime_runtime(&self, arg: ExprId) -> Result<Option<DfExpr>, LoweringError> {
        let null = || DfExpr::Literal(ScalarValue::Null, None);
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let null_str = || DfExpr::Literal(ScalarValue::Utf8(None), None);
        let (date_src, time_src, overrides, timezone) = match self.arena.get(arg) {
            IrExpr::MapLiteral(entries) => {
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let lower_or = |id: Option<ExprId>, default: &dyn Fn() -> DfExpr| match id {
                    Some(id) => self.lower(id),
                    None => Ok(default()),
                };
                let date_anchor = field("datetime").or_else(|| field("date"));
                let time_anchor = field("datetime").or_else(|| field("time"));
                let date_src = lower_or(date_anchor, &null)?;
                let time_src = lower_or(time_anchor, &null)?;
                let ov = |name: &str| lower_or(field(name), &null_i64);
                let overrides = [
                    ov("year")?,
                    ov("month")?,
                    ov("day")?,
                    ov("week")?,
                    ov("dayOfWeek")?,
                    ov("ordinalDay")?,
                    ov("quarter")?,
                    ov("dayOfQuarter")?,
                    ov("hour")?,
                    ov("minute")?,
                    ov("second")?,
                    ov("millisecond")?,
                    ov("microsecond")?,
                    ov("nanosecond")?,
                ];
                (
                    date_src,
                    time_src,
                    overrides,
                    lower_or(field("timezone"), &null_str)?,
                )
            }
            // A string literal that `const_datetime` rejected must not be lenient-
            // projected; fall through to the untyped path.
            IrExpr::Literal(IrLiteral::Str(_)) => return Ok(None),
            _ => {
                let base = self.lower(arg)?;
                (
                    base.clone(),
                    base,
                    std::array::from_fn(|_| null_i64()),
                    null_str(),
                )
            }
        };
        let mut call_args = Vec::with_capacity(17);
        call_args.push(date_src);
        call_args.push(time_src);
        call_args.extend(overrides);
        call_args.push(timezone);
        Ok(Some(CYPHER_DATETIME_PROJECT.call(call_args)))
    }

    /// Lower `date.truncate(unit, value [, map])` to a `cypher_date_truncate`
    /// call (`Temporal9`): truncate `value`'s date to `unit`, then apply the
    /// optional override map's components. (#920)
    pub(super) fn lower_date_truncate(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let [unit_id, value_id, rest @ ..] = args else {
            return Err(LoweringError::UnknownFunction("date.truncate".to_string()));
        };
        let unit = self.lower(*unit_id)?;
        let value = self.lower(*value_id)?;
        // The optional third argument is a component-override map. Overrides are
        // extracted at lowering time, so only a *literal* map is supported; a
        // non-literal third argument (e.g. `$m` or a variable) would otherwise
        // be silently dropped and return a subtly wrong date — error instead.
        let overrides: [DfExpr; 8] = match rest.first() {
            None => std::array::from_fn(|_| null_i64()),
            Some(map_id) => {
                let IrExpr::MapLiteral(entries) = self.arena.get(*map_id) else {
                    return Err(LoweringError::UnsupportedExpr(
                        "date.truncate override map must be a literal map".to_string(),
                    ));
                };
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let ov = |name: &str| match field(name) {
                    Some(id) => self.lower(id),
                    None => Ok(null_i64()),
                };
                [
                    ov("year")?,
                    ov("month")?,
                    ov("day")?,
                    ov("week")?,
                    ov("dayOfWeek")?,
                    ov("ordinalDay")?,
                    ov("quarter")?,
                    ov("dayOfQuarter")?,
                ]
            }
        };
        let mut call_args = Vec::with_capacity(10);
        call_args.push(value);
        call_args.push(unit);
        call_args.extend(overrides);
        Ok(CYPHER_DATE_TRUNCATE.call(call_args))
    }

    /// Lower `localtime.truncate(unit, value [, map])` to a `cypher_localtime_truncate`
    /// call (`Temporal9`): truncate `value`'s time-of-day to `unit`, then apply the
    /// optional override map's time components. (#920)
    pub(super) fn lower_localtime_truncate(
        &self,
        args: &[ExprId],
    ) -> Result<DfExpr, LoweringError> {
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let [unit_id, value_id, rest @ ..] = args else {
            return Err(LoweringError::UnknownFunction(
                "localtime.truncate".to_string(),
            ));
        };
        let unit = self.lower(*unit_id)?;
        let value = self.lower(*value_id)?;
        // Optional third arg is a literal component-override map (see `lower_date_truncate`).
        let overrides: [DfExpr; 6] = match rest.first() {
            None => std::array::from_fn(|_| null_i64()),
            Some(map_id) => {
                let IrExpr::MapLiteral(entries) = self.arena.get(*map_id) else {
                    return Err(LoweringError::UnsupportedExpr(
                        "localtime.truncate override map must be a literal map".to_string(),
                    ));
                };
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let ov = |name: &str| match field(name) {
                    Some(id) => self.lower(id),
                    None => Ok(null_i64()),
                };
                [
                    ov("hour")?,
                    ov("minute")?,
                    ov("second")?,
                    ov("millisecond")?,
                    ov("microsecond")?,
                    ov("nanosecond")?,
                ]
            }
        };
        let mut call_args = Vec::with_capacity(8);
        call_args.push(value);
        call_args.push(unit);
        call_args.extend(overrides);
        Ok(CYPHER_LOCALTIME_TRUNCATE.call(call_args))
    }

    /// Lower `localdatetime.truncate(unit, value [, map])` to a
    /// `cypher_localdatetime_truncate` call (`Temporal9`): truncate `value` to
    /// `unit` (date component for day-and-coarser units, time component for finer
    /// units), then apply the optional override map's date + time components. (#920)
    pub(super) fn lower_localdatetime_truncate(
        &self,
        args: &[ExprId],
    ) -> Result<DfExpr, LoweringError> {
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let [unit_id, value_id, rest @ ..] = args else {
            return Err(LoweringError::UnknownFunction(
                "localdatetime.truncate".to_string(),
            ));
        };
        let unit = self.lower(*unit_id)?;
        let value = self.lower(*value_id)?;
        // Optional third arg is a literal override map carrying date AND time
        // components (see `lower_date_truncate`).
        let overrides: [DfExpr; 14] = match rest.first() {
            None => std::array::from_fn(|_| null_i64()),
            Some(map_id) => {
                let IrExpr::MapLiteral(entries) = self.arena.get(*map_id) else {
                    return Err(LoweringError::UnsupportedExpr(
                        "localdatetime.truncate override map must be a literal map".to_string(),
                    ));
                };
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let ov = |name: &str| match field(name) {
                    Some(id) => self.lower(id),
                    None => Ok(null_i64()),
                };
                [
                    ov("year")?,
                    ov("month")?,
                    ov("day")?,
                    ov("week")?,
                    ov("dayOfWeek")?,
                    ov("ordinalDay")?,
                    ov("quarter")?,
                    ov("dayOfQuarter")?,
                    ov("hour")?,
                    ov("minute")?,
                    ov("second")?,
                    ov("millisecond")?,
                    ov("microsecond")?,
                    ov("nanosecond")?,
                ]
            }
        };
        let mut call_args = Vec::with_capacity(16);
        call_args.push(value);
        call_args.push(unit);
        call_args.extend(overrides);
        Ok(CYPHER_LOCALDATETIME_TRUNCATE.call(call_args))
    }

    /// Lower `time.truncate(unit, value [, map])` to a `cypher_time_truncate` call
    /// (`Temporal9`): truncate `value`'s time-of-day to `unit` (keeping its zone
    /// offset), then apply the override map's time components and optional
    /// `timezone`. (#920)
    pub(super) fn lower_time_truncate(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let null_str = || DfExpr::Literal(ScalarValue::Utf8(None), None);
        let [unit_id, value_id, rest @ ..] = args else {
            return Err(LoweringError::UnknownFunction("time.truncate".to_string()));
        };
        let unit = self.lower(*unit_id)?;
        let value = self.lower(*value_id)?;
        let (overrides, timezone): ([DfExpr; 6], DfExpr) = match rest.first() {
            None => (std::array::from_fn(|_| null_i64()), null_str()),
            Some(map_id) => {
                let IrExpr::MapLiteral(entries) = self.arena.get(*map_id) else {
                    return Err(LoweringError::UnsupportedExpr(
                        "time.truncate override map must be a literal map".to_string(),
                    ));
                };
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let ov = |name: &str| match field(name) {
                    Some(id) => self.lower(id),
                    None => Ok(null_i64()),
                };
                let tz = match field("timezone") {
                    Some(id) => self.lower(id)?,
                    None => null_str(),
                };
                (
                    [
                        ov("hour")?,
                        ov("minute")?,
                        ov("second")?,
                        ov("millisecond")?,
                        ov("microsecond")?,
                        ov("nanosecond")?,
                    ],
                    tz,
                )
            }
        };
        let mut call_args = Vec::with_capacity(9);
        call_args.push(value);
        call_args.push(unit);
        call_args.extend(overrides);
        call_args.push(timezone);
        Ok(CYPHER_TIME_TRUNCATE.call(call_args))
    }

    /// Lower `datetime.truncate(unit, value [, map])` to a `cypher_datetime_truncate`
    /// call (`Temporal9`): truncate `value` to `unit` (date for day-and-coarser
    /// units, time for finer units; keeping its zone), then apply the override
    /// map's date + time components and optional `timezone`. (#920)
    pub(super) fn lower_datetime_truncate(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let null_str = || DfExpr::Literal(ScalarValue::Utf8(None), None);
        let [unit_id, value_id, rest @ ..] = args else {
            return Err(LoweringError::UnknownFunction(
                "datetime.truncate".to_string(),
            ));
        };
        let unit = self.lower(*unit_id)?;
        let value = self.lower(*value_id)?;
        let (overrides, timezone): ([DfExpr; 14], DfExpr) = match rest.first() {
            None => (std::array::from_fn(|_| null_i64()), null_str()),
            Some(map_id) => {
                let IrExpr::MapLiteral(entries) = self.arena.get(*map_id) else {
                    return Err(LoweringError::UnsupportedExpr(
                        "datetime.truncate override map must be a literal map".to_string(),
                    ));
                };
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let ov = |name: &str| match field(name) {
                    Some(id) => self.lower(id),
                    None => Ok(null_i64()),
                };
                let tz = match field("timezone") {
                    Some(id) => self.lower(id)?,
                    None => null_str(),
                };
                (
                    [
                        ov("year")?,
                        ov("month")?,
                        ov("day")?,
                        ov("week")?,
                        ov("dayOfWeek")?,
                        ov("ordinalDay")?,
                        ov("quarter")?,
                        ov("dayOfQuarter")?,
                        ov("hour")?,
                        ov("minute")?,
                        ov("second")?,
                        ov("millisecond")?,
                        ov("microsecond")?,
                        ov("nanosecond")?,
                    ],
                    tz,
                )
            }
        };
        let mut call_args = Vec::with_capacity(17);
        call_args.push(value);
        call_args.push(unit);
        call_args.extend(overrides);
        call_args.push(timezone);
        Ok(CYPHER_DATETIME_TRUNCATE.call(call_args))
    }

    /// Lower `duration.between(a, b)` / `inMonths` / `inDays` / `inSeconds` to a
    /// `cypher_duration_between` call `[a, b, mode]`, where `mode` is the
    /// (lowercased) function name. (#920)
    pub(super) fn lower_duration_between(
        &self,
        name: &str,
        args: &[ExprId],
    ) -> Result<DfExpr, LoweringError> {
        let [a_id, b_id] = args else {
            return Err(LoweringError::UnknownFunction(name.to_string()));
        };
        let a = self.lower(*a_id)?;
        let b = self.lower(*b_id)?;
        Ok(CYPHER_DURATION_BETWEEN.call(vec![a, b, lit(name)]))
    }

    fn extract_temporal_fields(
        &self,
        entries: &[(String, ExprId)],
    ) -> Option<std::collections::HashMap<String, crate::temporal::TemporalField>> {
        let mut fields = std::collections::HashMap::with_capacity(entries.len());
        for (key, value) in entries {
            fields.insert(key.clone(), self.extract_temporal_field(*value)?);
        }
        Some(fields)
    }

    /// Read a single temporal map field value as a constant. A nested `date(…)`
    /// anchor (the `Temporal1` week forms) is rendered and re-parsed to a date.
    fn extract_temporal_field(&self, id: ExprId) -> Option<crate::temporal::TemporalField> {
        use crate::temporal::TemporalField;
        use graphforge_ir::expr::UnaryOpKind;
        match self.arena.get(id) {
            IrExpr::Literal(IrLiteral::Int(n)) => Some(TemporalField::Int(*n)),
            IrExpr::Literal(IrLiteral::Float(x)) => Some(TemporalField::Float(*x)),
            IrExpr::Literal(IrLiteral::Str(s)) => Some(TemporalField::Str(s.clone())),
            // A negative field (`days: -14`) lowers to unary-minus over a literal,
            // not a negative literal — fold it so the map still constant-folds.
            IrExpr::UnaryOp {
                op: UnaryOpKind::Neg,
                expr,
            } => match self.arena.get(*expr) {
                IrExpr::Literal(IrLiteral::Int(n)) => Some(TemporalField::Int(-n)),
                IrExpr::Literal(IrLiteral::Float(x)) => Some(TemporalField::Float(-x)),
                _ => None,
            },
            IrExpr::FunctionCall { name, args } if name == "date" => {
                if let [a] = args.as_slice()
                    && let IrExpr::Literal(IrLiteral::Str(s)) = self.arena.get(*a)
                {
                    crate::temporal::parse_date_string(s).map(TemporalField::Date)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Lower `datetime.fromepoch(seconds, nanoseconds)` /
    /// `datetime.fromepochmillis(milliseconds)` from integer-literal arguments
    /// to the canonical UTC datetime string. (#599)
    pub(super) fn lower_from_epoch(
        &self,
        name: &str,
        args: &[ExprId],
    ) -> Result<DfExpr, LoweringError> {
        self.try_from_epoch(name, args)
            .map(lit)
            .ok_or_else(|| LoweringError::UnknownFunction(name.to_string()))
    }

    fn try_from_epoch(&self, name: &str, args: &[ExprId]) -> Option<String> {
        match (name, args) {
            ("datetime.fromepoch", &[a, b]) => {
                crate::temporal::render_from_epoch(self.int_literal(a)?, self.int_literal(b)?)
            }
            ("datetime.fromepochmillis", &[a]) => {
                crate::temporal::render_from_epoch_millis(self.int_literal(a)?)
            }
            _ => None,
        }
    }

    /// Read an integer literal argument, or `None` if it isn't one.
    fn int_literal(&self, id: ExprId) -> Option<i64> {
        match self.arena.get(id) {
            IrExpr::Literal(IrLiteral::Int(n)) => Some(*n),
            _ => None,
        }
    }
}
