"""
Generate cross-language query parity fixtures.

Builds a representative set of `ftmq.Query` objects and dumps each with its
Python serialization on all four surfaces (`to_dict`, `to_params`, `to_string`,
`to_rql`). The TypeScript test suite (`js/tests/query.test.ts`) asserts that the
TS `Query` parses and reproduces these byte-for-byte (sorted-key surfaces) or
structurally (dict / rql).

Run: `.venv/bin/python scripts/gen_query_fixtures.py`
"""

import json
from pathlib import Path
from typing import Any

from ftmq import A, C, G, M, P, Query, Year
from ftmq.query.exceptions import QueryError

OUT = Path(__file__).parent.parent / "js" / "tests" / "fixtures" / "query_cases.json"


def jsonable(value: Any) -> Any:
    """Recursively convert sets to sorted lists so the dict is JSON-safe."""
    if isinstance(value, (set, frozenset)):
        return sorted(jsonable(v) for v in value)
    if isinstance(value, dict):
        return {k: jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    return value


def surface(fn) -> Any:
    try:
        return fn()
    except QueryError:
        return None


CASES: dict[str, Query] = {
    "flat_and": Query().where(M(schema="Person"), P(name="Jane")),
    "dataset_in": Query().where(M(dataset__in=["d1", "d2"])),
    "exclude_multi": Query().where(M(dataset__not_in=["a", "b"])),
    "nested_or": Query().where(
        M(schema="Person") & (G(countries="de") | G(countries="at"))
    ),
    "negation": Query().where(~P(name__ilike="jane")),
    "ranges": Query().where(P(date__gte="2023"), P(amountEur__lt="1000")),
    "prefix": Query().where(M(canonical_id__startswith="eu-")),
    "reverse": Query().where(G(entities="some-id")),
    "context": Query().where(C(origin="crawl")),
    "schemata": Query().where(M(schemata="LegalEntity")),
    "empty": Query().where(P(deathDate__null=True)),
    "sort_slice": Query().where(M(schema="Payment")).order_by("-date")[10:20],
    "sort_asc": Query().where(M(schema="Person")).order_by("name")[:25],
    "agg_ungrouped": Query()
    .where(M(schema="Payment"))
    .aggregate(A(min=P("date"), max=P("date"), sum=P("amountEur"))),
    "agg_grouped": Query()
    .where(M(schema="Payment"))
    .aggregate(A(sum=P("amountEur"), by=P("beneficiary")), A(count=M("id"))),
    "combined": Query()
    .where(M(dataset="donations"), M(schema="Payment"), P(date__gte="2010"))
    .order_by("-amountEur")[0:50]
    .aggregate(A(sum=P("amountEur"), by=Year())),
    # every family is addressable as an aggregation field, spelled as in the
    # filter grammar (`topics` the group vs `properties.topics` the property)
    "agg_group_field": Query().aggregate(A(count=M("id"), by=G("countries"))),
    "agg_families": Query().aggregate(
        A(count=P("topics"), by=[G("topics"), C("origin"), M("dataset")])
    ),
    # `select` is a projection alongside the filter tree, spelled with the same
    # field codec (`properties.<name>` / `group.<name>`)
    "select_props": Query()
    .where(M(schemata="Document"))
    .select(P("title"), P("fileName")),
    "select_group": Query().where(P(name="Jane")).select(G("countries")),
    "select_with_agg_and_slice": Query()
    .where(M(schema="Payment"))
    .aggregate(A(sum=P("amountEur"), by=P("beneficiary")))
    .order_by("-date")[0:25]
    .select(P("amountEur"), G("dates")),
}


def main() -> None:
    cases = []
    for name, q in CASES.items():
        params = surface(lambda q=q: {k: list(v) for k, v in q.to_params().items()})
        string = surface(lambda q=q: q.to_string())
        rql = surface(lambda q=q: q.to_rql())
        cases.append(
            {
                "name": name,
                "dict": jsonable(q.to_dict()),
                "params": params,
                "string": string,
                "rql": rql,
                # dicts after re-parsing each surface in Python: the params
                # surface is lossy for per-metric aggregation grouping (openaleph
                # facets broadcast to all metrics), so parse-parity compares
                # against these, not the original dict
                "params_dict": (
                    jsonable(Query.from_params(params).to_dict())
                    if params is not None
                    else None
                ),
                "rql_dict": (
                    jsonable(Query.from_rql(rql).to_dict()) if rql is not None else None
                ),
            }
        )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(cases, indent=2, ensure_ascii=False) + "\n")
    print(f"wrote {len(cases)} cases to {OUT}")


if __name__ == "__main__":
    main()
