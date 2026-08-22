import pytest
import sqlalchemy as sa
from followthemoney import Statement, StatementEntity, model
from sqlalchemy import column, literal_column, table
from sqlalchemy.sql.selectable import Select

from ftmq.query import A, C, G, M, P, Query, Year
from ftmq.query.sql import Sql, SqlSource, numeric_value, prune_by_schema
from ftmq.util import make_dataset, make_entity


def _literal(stmt) -> str:
    return str(stmt.compile(compile_kwargs={"literal_binds": True}))


# how a numeric read of a statement `value` renders (see `numeric_value`: the
# stored value is assumed to be in canonical number format already)
NUMERIC = _literal(numeric_value(literal_column("test_table.value")))


def _compare_str(s1, s2) -> bool:
    return " ".join(str(s1).split()).strip() == " ".join(str(s2).split()).strip()


def test_sql():
    q = Query().where(M(dataset__in=["other", "test"]), M(schema="Event"))
    q = q.where(P(date__gte=2023))
    # every field lifts to a `canonical_id` subquery: filters select *entities*,
    # so they AND with each other instead of competing for the same statement
    # row, and a matching entity is assembled from all of its statements
    ids = "SELECT DISTINCT test_table.canonical_id FROM test_table"
    whereclause = f"""WHERE test_table.canonical_id IN
        ({ids} WHERE test_table.dataset IN (__[POSTCOMPILE_dataset_1]))
    AND test_table.canonical_id IN ({ids} WHERE test_table.schema = :schema_1)
    AND test_table.canonical_id IN
        ({ids} WHERE test_table.prop = :prop_1 AND test_table.value >= :value_1)"""
    fields = """test_table.id, test_table.entity_id, test_table.canonical_id, test_table.prop,
    test_table.prop_type, test_table.schema, test_table.value, test_table.original_value,
    test_table.dataset, test_table.origin, test_table.lang, test_table.external,
    test_table.first_seen, test_table.last_seen"""
    # meta facets group over the rows of the matching entities
    assert isinstance(q.sql.canonical_ids, Select)
    assert _compare_str(
        q.sql.canonical_ids,
        f"""
        SELECT DISTINCT test_table.canonical_id
        FROM test_table {whereclause}
        """,
    )

    # the clause is already entity-level, so the statement select applies it
    # directly - no second pass through a `canonical_id IN (...)` wrapper
    assert isinstance(q.sql.statements, Select)
    assert _compare_str(
        q.sql.statements,
        f"""
        SELECT {fields} FROM test_table {whereclause}
        ORDER BY test_table.canonical_id
        """,
    )

    assert isinstance(q.sql.count, Select)
    assert _compare_str(
        q.sql.count,
        f"""
        SELECT count(DISTINCT test_table.canonical_id) AS count_1
        FROM test_table {whereclause}
        """,
    )

    # the clauses are entity-level already, so aggregates over "every row of a
    # matching entity" apply them directly rather than re-selecting the ids
    entity_clause = whereclause.removeprefix("WHERE")
    assert isinstance(q.sql.date_range, Select)
    assert _compare_str(
        q.sql.date_range,
        f"""
        SELECT min(test_table.value) AS min_1, max(test_table.value) AS max_1
        FROM test_table
        WHERE test_table.prop_type = :prop_type_1 AND {entity_clause}
        """,
    )

    # order by creates a join
    q = Query().where(M(dataset__in=["other", "test"]), M(schema="Event"))
    q = q.where(P(date__gte=2023)).order_by("name", ascending=False)
    # same three memberships, but the sort binds `prop` as :prop_1 first
    whereclause2 = whereclause.replace(":prop_1", ":prop_2")
    assert isinstance(q.sql.statements, Select)
    assert _compare_str(
        q.sql.statements,
        f"""
        SELECT {fields}, anon_1.canonical_id AS canonical_id_1, anon_1.sortable_value
        FROM test_table JOIN (SELECT test_table.canonical_id AS canonical_id, max(test_table.value) AS sortable_value
            FROM test_table
            WHERE test_table.prop = :prop_1 AND test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id
                FROM test_table {whereclause2})
            GROUP BY test_table.canonical_id
            ORDER BY sortable_value DESC, test_table.canonical_id)
        AS anon_1 ON test_table.canonical_id = anon_1.canonical_id
        ORDER BY anon_1.sortable_value DESC, test_table.canonical_id
        """,
    )

    # cast order by
    q = Query().order_by("amount")
    assert NUMERIC in _literal(q.sql.statements)

    # slice
    q = (
        Query()
        .where(M(dataset="test"))
        .where(M(dataset="other"), M(schema="Event"))
        .where(P(date__gte=2023))
    )
    assert str(q[:10].sql.canonical_ids).endswith("LIMIT :param_1")
    assert str(q[1:10].sql.canonical_ids).endswith("LIMIT :param_1 OFFSET :param_2")

    # ordered slice
    q = Query().where(M(dataset__in=["other", "test"]), M(schema="Event"))
    q = q.where(P(date__gte=2023)).order_by("name")
    assert not str(q[:10].sql.canonical_ids).endswith("LIMIT :param_1")
    assert not str(q[1:10].sql.canonical_ids).endswith("LIMIT :param_1 OFFSET :param_2")
    q = q[1:10]
    assert _compare_str(
        q.sql.statements,
        f"""
        SELECT {fields}, anon_1.canonical_id AS canonical_id_1, anon_1.sortable_value
        FROM test_table JOIN (SELECT test_table.canonical_id AS canonical_id, min(test_table.value) AS sortable_value
            FROM test_table
            WHERE test_table.prop = :prop_1 AND test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id
                FROM test_table {whereclause2})
            GROUP BY test_table.canonical_id
            ORDER BY sortable_value, test_table.canonical_id
            LIMIT :param_1 OFFSET :param_2)
        AS anon_1 ON test_table.canonical_id = anon_1.canonical_id
        ORDER BY anon_1.sortable_value, test_table.canonical_id
        """,
    )

    # aggregation
    q = q.aggregate(A(sum=P("amount")), A(max=P("date")))
    q = _literal(q.sql.aggregations)
    assert len(q.split("UNION")) == 2
    assert "SELECT 'properties.date', 'max', max(test_table.value) AS max" in q
    assert f"SELECT 'properties.amount', 'sum', sum({NUMERIC}) AS sum" in q

    q = _literal(Query().aggregate(A(avg=P("amount"))).sql.aggregations)
    assert f"SELECT 'properties.amount', 'avg', avg({NUMERIC}) AS avg" in q

    # min / max over a numeric property read as numbers, not strings
    q = _literal(Query().aggregate(A(min=P("amount"))).sql.aggregations)
    assert f"SELECT 'properties.amount', 'min', min({NUMERIC}) AS min" in q

    # `count` stays over the raw values - it needs no arithmetic
    q = _literal(Query().aggregate(A(count=P("amount"))).sql.aggregations)
    assert (
        "SELECT 'properties.amount', 'count', count(DISTINCT test_table.value) AS count"
        in q
    )

    q = _literal(Query().aggregate(A(count=P("location"))).sql.aggregations)
    assert (
        "SELECT 'properties.location', 'count', "
        "count(DISTINCT test_table.value) AS count" in q
    )

    # a meta field aggregates its own column, not the `value` of a `prop = id`
    # row (which holds the *unresolved* entity id, i.e. referents)
    q = _literal(Query().aggregate(A(count=M("id"))).sql.aggregations)
    assert "SELECT 'id', 'count', count(DISTINCT test_table.canonical_id) AS count" in q
    assert "test_table.prop = 'id'" not in q

    q = Query().where(P(date=2023))
    q = q.sql.get_group_counts(P("country"))
    res = q.compile(compile_kwargs={"literal_binds": True})
    assert _compare_str(
        res,
        """
        SELECT test_table.value, count(DISTINCT test_table.canonical_id) AS count
        FROM test_table
        WHERE test_table.prop = 'country' AND test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id
        FROM test_table
        WHERE test_table.prop = 'date' AND test_table.value = '2023') GROUP BY test_table.value ORDER BY count DESC
        """,
    )

    # grouped aggregations: one select per grouper, the specs' rows joined
    # against the distinct (entity, group value) pairs and grouped - one round
    # trip per grouper instead of one per group value
    q = (
        Query()
        .where(M(dataset="test"), M(schema="Project"))
        .aggregate(A(max=P("amountEur"), by=[P("country"), Year(), M("dataset")]))
    )
    assert q.sql.group_props == {P("country"), Year(), M("dataset")}
    res = q.sql.grouped_aggregations(Year()).compile(
        compile_kwargs={"literal_binds": True}
    )
    assert _compare_str(
        res,
        f"""
        SELECT 'properties.amountEur', 'max', anon_1.gval, max({NUMERIC}) AS max_1
        FROM test_table JOIN (SELECT DISTINCT test_table.canonical_id AS cid, substring(test_table.value, 1, 4) AS gval
        FROM test_table
        WHERE test_table.prop_type = 'date'
        AND test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id FROM test_table WHERE test_table.dataset = 'test')
        AND test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id FROM test_table WHERE test_table.schema = 'Project')) AS anon_1 ON test_table.canonical_id = anon_1.cid
        WHERE test_table.prop = 'amountEur' GROUP BY anon_1.gval
        """,
    )
    # a limit caps to the most frequent group values (by entity count)
    res = str(
        q.sql.grouped_aggregations(M("dataset"), limit=5).compile(
            compile_kwargs={"literal_binds": True}
        )
    )
    assert "ORDER BY count DESC" in res and "LIMIT 5" in res

    # reversed: the `entities` group is not special, it is the `entity`
    # prop-type group lifted to a canonical_id subquery like any other group
    q = Query().where(G(entities="my_id")).where(P(date=2023), M(schema="Event"))
    assert _compare_str(
        str(q.sql.statements.compile(compile_kwargs={"literal_binds": True})),
        f"""
        SELECT {fields} FROM test_table
        WHERE test_table.canonical_id IN ({ids} WHERE test_table.schema = 'Event')
        AND test_table.canonical_id IN
            ({ids} WHERE test_table.prop = 'date' AND test_table.value = '2023')
        AND test_table.canonical_id IN
            ({ids} WHERE test_table.prop_type = 'entity' AND test_table.value = 'my_id')
        ORDER BY test_table.canonical_id
        """,
    )

    # simplified if no properties or reversed (an unfiltered query compiles
    # `WHERE true` - never a bare zero-argument conjunction)
    q = Query()
    assert _compare_str(
        q.sql.statements,
        f"""
        SELECT {fields} FROM test_table WHERE true ORDER BY test_table.canonical_id
        """,
    )
    q = Query().where(M(dataset="foo"), M(schema="Person"))
    assert _compare_str(
        q.sql.statements,
        f"""
        SELECT {fields} FROM test_table
        WHERE test_table.canonical_id IN ({ids} WHERE test_table.dataset = :dataset_1)
        AND test_table.canonical_id IN ({ids} WHERE test_table.schema = :schema_1)
        ORDER BY test_table.canonical_id
        """,
    )
    # the row-level escape hatch keeps the same predicates un-lifted, for
    # callers that want the matching statements rather than the matching
    # entities' statements
    assert _compare_str(
        q.sql.row_statements,
        f"""
        SELECT {fields} FROM test_table
        WHERE test_table.dataset = :dataset_1 AND test_table.schema = :schema_1
        ORDER BY test_table.canonical_id
        """,
    )

    # but we need complex query if we want a limit:
    assert "canonical_id IN" in str(q[:10].sql.statements)


def test_sql_ids():
    # `entity_id` is not the source's id column: on a resolved store one
    # entity's rows can carry several of them, so it lifts to a membership -
    # otherwise the entity would come back assembled from just those rows
    q = Query().where(M(entity_id="eu-authorities-chafea"))
    assert (
        "WHERE test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id"
        in " ".join(str(q.sql.statements).split())
    )
    assert "WHERE test_table.entity_id = :entity_id_1" in " ".join(
        str(q.sql.statements).split()
    )
    # `canonical_id` *is* the id column: the predicate already holds for every
    # row of a matching entity, so it needs no subquery
    q = Query().where(M(canonical_id="eu-authorities-chafea"))
    assert "WHERE test_table.canonical_id = :canonical_id_1" in str(q.sql.statements)

    # the different id fields AND together (like any two different fields);
    # they used to be OR-ed into one clause
    q = Query().where(M(entity_id="a", canonical_id="b"))
    assert (
        "WHERE test_table.canonical_id = :canonical_id_1"
        " AND test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id"
        " FROM test_table WHERE test_table.entity_id = :entity_id_1)"
        in " ".join(str(q.sql.canonical_ids).split())
    )


def test_sql_comparators():
    def lit(q: Query) -> str:
        compiled = q.sql.canonical_ids.compile(compile_kwargs={"literal_binds": True})
        return " ".join(str(compiled).split())

    # notlike / notilike are real comparators (they used to fall through to
    # arbitrary column methods and match nearly everything)
    assert "NOT LIKE '%' || 'acme' || '%' ESCAPE '/'" in lit(
        Query().where(P(name__notlike="acme"))
    )
    assert "NOT LIKE '%' || lower('acme') || '%' ESCAPE '/'" in lit(
        Query().where(P(name__notilike="acme"))
    )

    # `%` / `_` in values are escaped so they match literally, like the
    # in-memory substring test
    assert "LIKE '%' || '100/%' || '%' ESCAPE '/'" in lit(
        Query().where(P(name__like="100%"))
    )
    assert "LIKE 'eu/_' || '%' ESCAPE '/'" in lit(
        Query().where(M(entity_id__startswith="eu_"))
    )

    # bool typed columns compare typed values - the leaf layer stringifies,
    # which broke boolean columns on every backend differently
    assert "external = false" in lit(Query().where(C(external=False)))
    assert "external = true" in lit(Query().where(C(external=True)))


def test_sql_context_multi_key():
    def compile(q: Query) -> str:
        return " ".join(str(q.sql.canonical_ids).split())

    # AND-ed context columns co-refer: one statement row carries both, so they
    # share a single membership sub-select instead of getting one each
    q = Query().where(C(origin="x"), C(lang="en"))
    compiled = compile(q)
    assert compiled.count("SELECT DISTINCT test_table.canonical_id") == 2
    assert "test_table.lang = :lang_1 AND test_table.origin = :origin_1" in compiled

    # so a range over one column restricts, instead of being satisfied by two
    # unrelated rows
    q = Query().where(C(first_seen__gte="2024-01-01"), C(first_seen__lt="2024-02-01"))
    compiled = compile(q)
    assert compiled.count("SELECT DISTINCT test_table.canonical_id") == 2
    assert (
        "test_table.first_seen >= :first_seen_1 "
        "AND test_table.first_seen < :first_seen_2" in compiled
    )

    # under OR they stay independent - two rows may satisfy the disjunction
    compiled = compile(Query().where(C(origin="x") | C(lang="en")))
    assert compiled.count("SELECT DISTINCT test_table.canonical_id") == 3
    assert " OR test_table.canonical_id IN " in compiled

    # a negated conjunction negates the joined clause: no row is both
    compiled = compile(Query().where(~(C(origin="x") & C(lang="en"))))
    assert compiled.count("SELECT DISTINCT test_table.canonical_id") == 2
    assert "canonical_id NOT IN" in compiled
    assert "test_table.lang = :lang_1 AND test_table.origin = :origin_1" in compiled

    # an absence test cannot join the conjunction - it stays an anti-join
    compiled = compile(Query().where(C(origin__null=True), C(lang="en")))
    assert compiled.count("SELECT DISTINCT test_table.canonical_id") == 3
    assert "test_table.origin IS NOT NULL" in compiled

    # other families keep their per-leaf entity semantics: a merged entity's
    # schema and its statements' origins are not properties of one row
    compiled = compile(Query().where(C(origin="x"), M(schema="Person")))
    assert compiled.count("SELECT DISTINCT test_table.canonical_id") == 3


def test_sql_origins():
    q = Query().where(C(origin="test"))
    assert "WHERE test_table.origin = :origin_1" in str(q.sql.statements)


def test_sql_boolean_tree():
    def where(q: Query) -> str:
        return str(q.sql.canonical_ids.compile(compile_kwargs={"literal_binds": True}))

    ids = "SELECT DISTINCT test_table.canonical_id FROM test_table"

    # different props AND together - one row cannot hold two props, so each
    # field lifts to its own entity-level sub-select
    assert _compare_str(
        where(Query().where(P(name="jane"), P(country="de"))),
        f"""
        {ids} WHERE test_table.canonical_id IN
            ({ids} WHERE test_table.prop = 'country' AND test_table.value = 'de')
        AND test_table.canonical_id IN
            ({ids} WHERE test_table.prop = 'name' AND test_table.value = 'jane')
        """,
    )

    # an OR node composes the same entity-level predicates
    assert _compare_str(
        where(Query().where(P(name="jane") | G(countries="de"))),
        f"""
        {ids} WHERE test_table.canonical_id IN
            ({ids} WHERE test_table.prop = 'name' AND test_table.value = 'jane')
        OR test_table.canonical_id IN
            ({ids} WHERE test_table.prop_type = 'country' AND test_table.value = 'de')
        """,
    )

    # a negation excludes the matching entities (it used to be dropped, which
    # silently returned exactly the entities the query asked to exclude)
    assert _compare_str(
        where(Query().where(~P(name="jane"))),
        f"""
        {ids} WHERE (test_table.canonical_id NOT IN
            ({ids} WHERE test_table.prop = 'name' AND test_table.value = 'jane'))
        """,
    )

    # nesting: a flat leaf ANDs with a nested OR group
    assert _compare_str(
        where(Query().where(M(schema="Person") & (P(name="jane") | P(name="joe")))),
        f"""
        {ids} WHERE test_table.canonical_id IN ({ids} WHERE test_table.schema = 'Person')
        AND (test_table.canonical_id IN
            ({ids} WHERE test_table.prop = 'name' AND test_table.value = 'jane')
        OR test_table.canonical_id IN
            ({ids} WHERE test_table.prop = 'name' AND test_table.value = 'joe'))
        """,
    )

    # `M(id=...)` targets the source's entity-identity column, not the
    # statement's own `id` column
    assert "WHERE test_table.canonical_id = :canonical_id_1" in str(
        Query().where(M(id="foo")).sql.statements
    )

    # a negated empty node matches nothing (it used to compile to TRUE and
    # return the entire store)
    assert _compare_str(where(Query().where(~M())), f"{ids} WHERE false")

    # chained same-field filters AND, like the in-memory evaluator (the flat
    # collectors used to OR them; alternatives are spelled `__in`)
    assert _compare_str(
        where(Query().where(M(dataset="d1")).where(M(dataset="d2"))),
        f"""
        {ids} WHERE test_table.canonical_id IN ({ids} WHERE test_table.dataset = 'd1')
        AND test_table.canonical_id IN ({ids} WHERE test_table.dataset = 'd2')
        """,
    )

    # `schema__not` / `schemata__not` compile as entity-level anti-joins:
    # in-memory they test the entity's single resolved schema, and a row
    # predicate would match any merged entity with a row outside the set
    assert _compare_str(
        where(Query().where(M(schema__not="Person"))),
        f"""
        {ids} WHERE (test_table.canonical_id NOT IN
            ({ids} WHERE test_table.schema IN ('Person')))
        """,
    )
    assert "NOT IN" in where(Query().where(M(schemata__not="Organization")))


def test_sql_null():
    # `null` is a presence test, not a value comparison: it must never end up
    # as `value IS true/false` (silently empty on sqlite, a cast error on duckdb)
    def where(q: Query) -> str:
        return str(q.sql.canonical_ids.compile(compile_kwargs={"literal_binds": True}))

    # a prop is present if the entity has any row for it ...
    assert _compare_str(
        where(Query().where(P(name__null=False))),
        """
        SELECT DISTINCT test_table.canonical_id FROM test_table
        WHERE test_table.canonical_id IN
            (SELECT DISTINCT test_table.canonical_id FROM test_table WHERE test_table.prop = 'name')
        """,
    )
    # ... and absent only if it has none, which lifts to an anti-join
    assert _compare_str(
        where(Query().where(P(name__null=True))),
        """
        SELECT DISTINCT test_table.canonical_id FROM test_table
        WHERE (test_table.canonical_id NOT IN
            (SELECT DISTINCT test_table.canonical_id FROM test_table WHERE test_table.prop = 'name'))
        """,
    )

    # groups test the same way on the `prop_type` column
    assert _compare_str(
        where(Query().where(G(countries__null=False))),
        """
        SELECT DISTINCT test_table.canonical_id FROM test_table
        WHERE test_table.canonical_id IN
            (SELECT DISTINCT test_table.canonical_id FROM test_table WHERE test_table.prop_type = 'country')
        """,
    )
    assert _compare_str(
        where(Query().where(G(countries__null=True))),
        """
        SELECT DISTINCT test_table.canonical_id FROM test_table
        WHERE (test_table.canonical_id NOT IN
            (SELECT DISTINCT test_table.canonical_id FROM test_table WHERE test_table.prop_type = 'country'))
        """,
    )

    # a context column is a real column, so presence is a NULL check on it -
    # lifted, like every other leaf, to the entities having such a row
    assert _compare_str(
        where(Query().where(C(origin__null=False))),
        """
        SELECT DISTINCT test_table.canonical_id FROM test_table
        WHERE test_table.canonical_id IN
            (SELECT DISTINCT test_table.canonical_id FROM test_table
             WHERE test_table.origin IS NOT NULL)
        """,
    )
    assert _compare_str(
        where(Query().where(C(origin__null=True))),
        """
        SELECT DISTINCT test_table.canonical_id FROM test_table
        WHERE (test_table.canonical_id NOT IN
            (SELECT DISTINCT test_table.canonical_id FROM test_table WHERE test_table.origin IS NOT NULL))
        """,
    )

    # combined with other filters
    assert _compare_str(
        where(Query().where(M(schema="Person"), P(name__null=False))),
        """
        SELECT DISTINCT test_table.canonical_id FROM test_table
        WHERE test_table.canonical_id IN
            (SELECT DISTINCT test_table.canonical_id FROM test_table WHERE test_table.schema = 'Person')
        AND test_table.canonical_id IN
            (SELECT DISTINCT test_table.canonical_id FROM test_table WHERE test_table.prop = 'name')
        """,
    )


# a partitioned statement table: `bucket` is derived from the schema (as in the
# lake store), `shard` from the dataset
PARTITIONED = table(
    "lake_table",
    column("canonical_id"),
    column("dataset"),
    column("schema"),
    column("prop"),
    column("prop_type"),
    column("value"),
    column("bucket"),
    column("shard"),
)


def _bucket(schema_name: str) -> str:
    return "interval" if model[schema_name].is_a("Interval") else "thing"


PARTITIONED_SOURCE = SqlSource(
    PARTITIONED,
    prune={
        "bucket": prune_by_schema(_bucket),
        "shard": lambda q: {f"s-{d}" for d in q.dataset_names},
        # a rule for a column this table doesn't have is ignored
        "nope": lambda q: {"x"},
    },
)


def test_sql_prune():
    def where(q: Query) -> str:
        return _literal(Sql(q, PARTITIONED_SOURCE).canonical_ids)

    # a positive schema filter prunes its partitions ...
    assert "lake_table.bucket IN ('thing')" in where(Query().where(M(schema="Person")))
    # ... an is-a filter over all its (non-abstract) descendants ...
    assert "lake_table.bucket IN ('interval')" in where(
        Query().where(M(schemata="Interval"))
    )
    # ... and several schemata over every partition they live in (sorted, so
    # the compiled sql is deterministic)
    assert "lake_table.bucket IN ('interval', 'thing')" in where(
        Query().where(M(schema__in=["Payment", "Person"]))
    )

    # no schema filter, nothing to prune
    assert "bucket" not in where(Query().where(P(name="jane")))

    # pruning is unsound for anything but a positive flat conjunct
    assert "bucket" not in where(Query().where(M(schema__not="Person")))
    assert "bucket" not in where(Query().where(~M(schema="Person")))
    assert "bucket" not in where(
        Query().where(M(schema="Person") | M(schema="Company"))
    )

    # every rule of the source compiles its own predicate, and one for a column
    # the table doesn't have is skipped
    compiled = where(Query().where(M(schema="Person"), M(dataset="test")))
    assert "lake_table.bucket IN ('thing')" in compiled
    assert "lake_table.shard IN ('s-test')" in compiled
    assert "nope" not in compiled

    # the default source has no prune rules
    assert "bucket" not in _literal(Query().where(M(schema="Person")).sql.canonical_ids)


# a real statement table to run the co-reference cases against: asserting
# entity ids is what actually pins the semantics, the compiled sql only shows
# how it gets there
COREF = sa.Table(
    "coref",
    sa.MetaData(),
    sa.Column("id", sa.Unicode(255), primary_key=True),
    sa.Column("entity_id", sa.Unicode(255)),
    sa.Column("canonical_id", sa.Unicode(255)),
    sa.Column("schema", sa.Unicode(255)),
    sa.Column("prop", sa.Unicode(255)),
    sa.Column("prop_type", sa.Unicode(255)),
    sa.Column("value", sa.Unicode()),
    sa.Column("dataset", sa.Unicode(255)),
    sa.Column("origin", sa.Unicode(255)),
    sa.Column("first_seen", sa.Unicode(255)),
)

# (entity, schema, prop, value, dataset, origin, first_seen)
COREF_ROWS = [
    # spans the window on two different statements, is in neither dataset twice
    ("doc-split", "Document", "title", "a", "d1", "crawl", "2020-01-01"),
    ("doc-split", "Document", "fileName", "b", "d1", "bulk", "2026-08-22"),
    # one statement that is both crawl and fresh
    ("doc-hit", "Document", "title", "c", "d1", "crawl", "2026-08-22"),
    # a merged entity: `Person` rows and `crawl` rows come from different
    # datasets, so no single row is both
    ("merged", "LegalEntity", "name", "e", "d1", "crawl", "2020-01-01"),
    ("merged", "Person", "name", "e", "d2", "bulk", "2020-01-01"),
    # a payment dated on both sides of october 2024, but never inside it
    ("pay-split", "Payment", "date", "2023-01-01", "d1", "crawl", "2020-01-01"),
    ("pay-split", "Payment", "date", "2025-06-01", "d1", "crawl", "2020-01-01"),
    # ... and one dated inside it
    ("pay-hit", "Payment", "date", "2024-10-05", "d1", "crawl", "2020-01-01"),
]


@pytest.fixture(scope="module")
def coref_engine():
    engine = sa.create_engine("sqlite://")
    COREF.metadata.create_all(engine)
    rows = [
        {
            "id": str(ix),
            "entity_id": entity,
            "canonical_id": entity,
            "schema": schema,
            "prop": prop,
            "prop_type": "date" if prop == "date" else "name",
            "value": value,
            "dataset": dataset,
            "origin": origin,
            "first_seen": first_seen,
        }
        for ix, (entity, schema, prop, value, dataset, origin, first_seen) in enumerate(
            COREF_ROWS
        )
    ]
    with engine.begin() as conn:
        conn.execute(sa.insert(COREF), rows)
    return engine


def test_sql_coreference(coref_engine):
    """Conditions that could hold of one statement row must hold of the same
    row - and the two evaluators have to agree on which those are."""

    def ids(q: Query) -> list[str]:
        with coref_engine.connect() as conn:
            return sorted(
                r[0] for r in conn.execute(Sql(q, SqlSource(COREF)).canonical_ids)
            )

    def memberships(q: Query) -> int:
        return _literal(Sql(q, SqlSource(COREF)).canonical_ids).count("SELECT DISTINCT")

    # --- joins: distinct row-scoped columns share a row ---------------------
    # the case that started this: an old crawl statement plus a fresh statement
    # of another origin is not a fresh crawl statement
    q = Query().where(C(origin="crawl"), C(first_seen__gte="2026-08-22"))
    assert ids(q) == ["doc-hit"]
    assert memberships(q) == 2  # the outer select plus one shared membership

    # `dataset` is row-scoped too, so it joins them
    assert ids(Query().where(M(dataset="d2"), C(origin="crawl"))) == []
    assert ids(Query().where(M(dataset="d1"), C(origin="crawl"))) == [
        "doc-hit",
        "doc-split",
        "merged",
        "pay-hit",
        "pay-split",
    ]

    # --- joins: bounds on one field describe one value ----------------------
    window = Query().where(P(date__gte="2024-10"), P(date__lt="2024-11"))
    assert ids(window) == ["pay-hit"]
    assert memberships(window) == 2
    # ... for a prop-type group as well
    assert ids(Query().where(G(dates__gte="2024-10"), G(dates__lt="2024-11"))) == [
        "pay-hit"
    ]
    # ... and a row-scoped range joins the other row-scoped columns with it
    q = Query().where(
        C(origin="crawl"),
        C(first_seen__gte="2026-01-01"),
        C(first_seen__lt="2027-01-01"),
    )
    assert ids(q) == ["doc-hit"]
    assert memberships(q) == 2

    # --- no join: entity-wide facts ----------------------------------------
    # a merged entity's schema is the join over its rows, so a `Person` filter
    # must not be forced onto the same row as the origin filter
    assert ids(Query().where(C(origin="crawl"), M(schema="Person"))) == ["merged"]
    assert memberships(Query().where(C(origin="crawl"), M(schema="Person"))) == 3

    # --- no join: repeated equality keeps set semantics ---------------------
    # "present in both datasets", not "one statement in two datasets"
    assert ids(Query().where(M(dataset="d1")).where(M(dataset="d2"))) == ["merged"]
    # "has both origins", not "one statement with two origins"
    assert ids(Query().where(C(origin="crawl")).where(C(origin="bulk"))) == [
        "doc-split",
        "merged",
    ]
    # a split field does not co-refer with anything else either
    q = Query().where(M(dataset="d1")).where(M(dataset="d2")).where(C(origin="crawl"))
    assert ids(q) == ["merged"]

    # --- no join: different props never share a row -------------------------
    assert ids(Query().where(P(title="a"), P(fileName="b"))) == ["doc-split"]

    # --- a mixed-comparator group is conservatively not joined --------------
    q = Query().where(C(first_seen__gte="2020-01-01"), C(first_seen__not="2026-08-22"))
    assert memberships(q) == 3

    # --- OR never joins -----------------------------------------------------
    assert (
        memberships(Query().where(C(origin="crawl") | C(first_seen__gte="2026"))) == 3
    )

    # --- a negated conjunction negates the joined clause --------------------
    q = Query().where(~(C(origin="crawl") & C(first_seen__gte="2026-08-22")))
    assert "NOT IN" in _literal(Sql(q, SqlSource(COREF)).canonical_ids)
    assert "doc-hit" not in ids(q)

    # --- an absence test stays an anti-join ---------------------------------
    q = Query().where(C(origin__null=True), C(first_seen__gte="2020-01-01"))
    assert memberships(q) == 3

    # --- the view scope stays its own conjunct ------------------------------
    # scoping selects entities; it must not require the *matching* statement to
    # live in a scoped dataset
    scoped = Sql(Query().where(C(origin="crawl")), SqlSource(COREF), scope=["d2"])
    with coref_engine.connect() as conn:
        assert sorted(r[0] for r in conn.execute(scoped.canonical_ids)) == ["merged"]


def test_coreference_in_memory():
    """The in-memory evaluator reads the same rule as the SQL one."""
    span = make_entity(
        {
            "id": "pay-split",
            "schema": "Payment",
            "datasets": ["d1"],
            "properties": {"date": ["2023-01-01", "2025-06-01"]},
        }
    )
    hit = make_entity(
        {
            "id": "pay-hit",
            "schema": "Payment",
            "datasets": ["d1"],
            "properties": {"date": ["2024-10-05"]},
        }
    )
    # bounds co-refer: one date has to fall inside the window
    window = Query().where(P(date__gte="2024-10"), P(date__lt="2024-11"))
    assert not window.apply(span)
    assert window.apply(hit)
    # ... the same for a prop-type group
    group_window = Query().where(G(dates__gte="2024-10"), G(dates__lt="2024-11"))
    assert not group_window.apply(span)
    assert group_window.apply(hit)

    both = make_entity(
        {
            "id": "e",
            "schema": "Person",
            "datasets": ["d1", "d2"],
            "properties": {"name": ["a", "b"]},
        }
    )
    # repeated equality keeps set semantics, as in SQL
    assert Query().where(M(dataset="d1")).where(M(dataset="d2")).apply(both)
    assert Query().where(P(name="a")).where(P(name="b")).apply(both)
    # a mixed-comparator group is not joined, so each condition stands alone
    assert Query().where(P(date__gte="2024-10"), P(date__not="2024-10-05")).apply(span)
    # OR is unaffected
    assert (Query().where(P(date__gte="2025") | P(date__lt="2024"))).apply(span)


def _statement_entity(entity_id: str, rows: list[tuple[str, str, str, str, str]]):
    """Build a `StatementEntity` from raw rows, so the in-memory evaluator sees
    the same statements the SQL one does."""
    statements = []
    for prop, value, dataset, origin, first_seen in rows:
        statement = Statement(
            entity_id=entity_id,
            canonical_id=entity_id,
            prop=prop,
            value=value,
            dataset=dataset,
            schema="Document",
        )
        statement.origin = origin
        statement.first_seen = first_seen
        statements.append(statement)
    return StatementEntity.from_statements(make_dataset("d"), statements)


def test_coreference_in_memory_row_scoped():
    """Row-scoped columns co-refer in memory too: a statement entity carries
    its rows, so the correlation the SQL backends read is available here."""
    split = _statement_entity(
        "doc-split",
        [
            ("title", "a", "d1", "crawl", "2020-01-01"),
            ("fileName", "b", "d1", "bulk", "2026-08-22"),
        ],
    )
    hit = _statement_entity("doc-hit", [("title", "c", "d1", "crawl", "2026-08-22")])

    # `C` used to match nothing at all on a statement entity - it read
    # `entity.context`, which `StatementEntity` never sets
    assert Query().where(C(origin="crawl")).apply(split)
    assert not Query().where(C(origin="nope")).apply(split)

    # distinct row-scoped columns have to hold for one statement
    fresh_crawl = Query().where(C(origin="crawl"), C(first_seen__gte="2026-08-22"))
    assert not fresh_crawl.apply(split)
    assert fresh_crawl.apply(hit)
    # ... `dataset` among them
    assert Query().where(M(dataset="d1"), C(origin="crawl")).apply(hit)
    assert not Query().where(M(dataset="d2"), C(origin="crawl")).apply(hit)

    # repeated equality keeps set semantics
    assert Query().where(C(origin="crawl")).where(C(origin="bulk")).apply(split)
    # an entity-wide fact does not join the row conjunction
    assert Query().where(M(schema="Document"), C(origin="crawl")).apply(split)
    # OR stays independent
    assert Query().where(C(origin="crawl") | C(first_seen__gte="2026")).apply(split)

    # an entity without statements (a json stream) has only the aggregated
    # context dict, so its conditions are tested one by one
    aggregated = make_entity(
        {
            "id": "doc-split",
            "schema": "Document",
            "datasets": ["d1"],
            "origin": ["crawl", "bulk"],
            "first_seen": "2026-08-22",
            "properties": {"title": ["a"]},
        }
    )
    assert Query().where(C(origin="crawl")).apply(aggregated)
    assert fresh_crawl.apply(aggregated)


def test_sql_select_projection():
    q = Query().where(M(schema="Person")).select(P("name"), G("countries"))
    statements = _literal(Sql(q, SqlSource(COREF)).statements)
    # the projection is a row predicate on the statement fetch ...
    assert "coref.prop = 'name'" in statements
    assert "coref.prop_type = 'country'" in statements
    # ... and the entity's own id statement always comes back, so an entity
    # holding none of the selected properties is not silently dropped
    assert "coref.prop = 'id'" in statements

    # it must not reach anything that decides *which* entities match
    for select in (
        Sql(q, SqlSource(COREF)).canonical_ids,
        Sql(q, SqlSource(COREF)).count,
    ):
        assert "coref.prop" not in _literal(select)

    # nor the aggregations, which read the whole entity
    agg = q.aggregate(A(count=M("id"), by=G("countries")))
    assert _literal(Sql(agg, SqlSource(COREF)).aggregations) == _literal(
        Sql(
            Query()
            .where(M(schema="Person"))
            .aggregate(A(count=M("id"), by=G("countries"))),
            SqlSource(COREF),
        ).aggregations
    )

    # a sort reads the sortable value from the unprojected table, so ordering
    # by a property that was projected away still works
    sorted_sql = _literal(Sql(q.order_by("-birthDate"), SqlSource(COREF)).statements)
    assert "coref.prop = 'birthDate'" in sorted_sql
    assert "coref.prop = 'id'" in sorted_sql

    # a slice routes through the canonical_ids sub-select, which stays unprojected
    sliced = _literal(Sql(q[:10], SqlSource(COREF)).statements)
    assert "coref.prop = 'id'" in sliced

    # the row-level escape hatch is projected too
    assert "coref.prop = 'id'" in _literal(Sql(q, SqlSource(COREF)).row_statements)

    # without a selection nothing changes
    plain = Query().where(M(schema="Person"))
    assert (
        "coref.prop"
        not in _literal(Sql(plain, SqlSource(COREF)).statements).split("WHERE", 1)[1]
    )
