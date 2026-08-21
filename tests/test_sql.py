from followthemoney import model
from sqlalchemy import column, literal_column, table
from sqlalchemy.sql.selectable import Select

from ftmq.query import A, C, G, M, P, Query, Year
from ftmq.query.sql import Sql, SqlSource, numeric_value, prune_by_schema


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
    # the property filter lifts to a `canonical_id` subquery so it ANDs with
    # the other fields instead of competing for the same statement row
    whereclause = """WHERE test_table.dataset IN (__[POSTCOMPILE_dataset_1])
    AND test_table.schema = :schema_1
    AND test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id
        FROM test_table WHERE test_table.prop = :prop_1 AND test_table.value >= :value_1)"""
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

    assert isinstance(q.sql.statements, Select)
    assert _compare_str(
        q.sql.statements,
        f"""
        SELECT {fields} FROM test_table
        WHERE test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id FROM test_table {whereclause})
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

    assert isinstance(q.sql.date_range, Select)
    assert _compare_str(
        q.sql.date_range,
        f"""
        SELECT min(test_table.value) AS min_1, max(test_table.value) AS max_1
        FROM test_table
        WHERE test_table.prop_type = :prop_type_1 AND test_table.canonical_id IN
        (SELECT DISTINCT test_table.canonical_id FROM test_table {whereclause})
        """,
    )

    # order by creates a join
    q = Query().where(M(dataset__in=["other", "test"]), M(schema="Event"))
    q = q.where(P(date__gte=2023)).order_by("name", ascending=False)
    assert isinstance(q.sql.statements, Select)
    assert _compare_str(
        q.sql.statements,
        f"""
        SELECT {fields}, anon_1.canonical_id AS canonical_id_1, anon_1.sortable_value
        FROM test_table JOIN (SELECT test_table.canonical_id AS canonical_id, max(test_table.value) AS sortable_value
            FROM test_table
            WHERE test_table.prop = :prop_1 AND test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id
                FROM test_table WHERE test_table.dataset IN (__[POSTCOMPILE_dataset_1])
                AND test_table.schema = :schema_1 AND test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id
                    FROM test_table WHERE test_table.prop = :prop_2 AND test_table.value >= :value_1))
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
                FROM test_table WHERE test_table.dataset IN (__[POSTCOMPILE_dataset_1])
                AND test_table.schema = :schema_1 AND test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id
                    FROM test_table WHERE test_table.prop = :prop_2 AND test_table.value >= :value_1))
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
        WHERE test_table.prop_type = 'date' AND test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id
        FROM test_table
        WHERE test_table.dataset = 'test' AND test_table.schema = 'Project')) AS anon_1 ON test_table.canonical_id = anon_1.cid
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
        WHERE test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id
        FROM test_table
        WHERE test_table.schema = 'Event'
        AND test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id
            FROM test_table WHERE test_table.prop = 'date' AND test_table.value = '2023')
        AND test_table.canonical_id IN (SELECT DISTINCT test_table.canonical_id
            FROM test_table WHERE test_table.prop_type = 'entity' AND test_table.value = 'my_id'))
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
        SELECT {fields} FROM test_table WHERE
        test_table.dataset = :dataset_1 AND test_table.schema = :schema_1 ORDER
        BY test_table.canonical_id
        """,
    )

    # but we need complex query if we want a limit:
    assert "canonical_id IN" in str(q[:10].sql.statements)


def test_sql_ids():
    q = Query().where(M(entity_id="eu-authorities-chafea"))
    assert "WHERE test_table.entity_id = :entity_id_1" in str(q.sql.statements)
    q = Query().where(M(canonical_id="eu-authorities-chafea"))
    assert "WHERE test_table.canonical_id = :canonical_id_1" in str(q.sql.statements)

    # the different id fields AND together (like any two different fields);
    # they used to be OR-ed into one clause
    q = Query().where(M(entity_id="a", canonical_id="b"))
    assert (
        "WHERE test_table.canonical_id = :canonical_id_1"
        " AND test_table.entity_id = :entity_id_1"
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
    # different context columns AND at the entity level: `origin` and `lang`
    # may be satisfied by two different statement rows
    q = Query().where(C(origin="x"), C(lang="en"))
    compiled = " ".join(str(q.sql.canonical_ids).split())
    assert compiled.count("SELECT DISTINCT test_table.canonical_id") == 3
    assert "test_table.lang = :lang_1" in compiled
    assert "test_table.origin = :origin_1" in compiled


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

    # a context column is a real column, so presence is a NULL check on it
    assert _compare_str(
        where(Query().where(C(origin__null=False))),
        "SELECT DISTINCT test_table.canonical_id FROM test_table WHERE test_table.origin IS NOT NULL",
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
        WHERE test_table.schema = 'Person' AND test_table.canonical_id IN
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
