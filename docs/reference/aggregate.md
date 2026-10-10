# `ftmq.aggregate`

Fast paths to merge trusted entity data without building FtM objects (and with that without value validation): statement dicts or entity fragment dicts into [`EntityDict`][ftmq.aggregate.EntityDict]s. Both expect an input stream sorted by entity id, and both merge conflicting schemata leniently via [`merge_schema`][ftmq.aggregate.merge_schema]. Consumers that need more than the entity dict (the statements, the `first_seen` bounds, a `StatementEntity`) iterate [`EntityPayload`][ftmq.aggregate.EntityPayload]s instead, from statements via [`aggregate_statement_payloads`][ftmq.aggregate.aggregate_statement_payloads] or from entity dicts via [`EntityPayload.from_dict`][ftmq.aggregate.EntityPayload.from_dict].

::: ftmq.aggregate.aggregate_statements_unsafe

::: ftmq.aggregate.aggregate_fragments_unsafe

::: ftmq.aggregate.EntityDict

::: ftmq.aggregate.aggregate_statement_payloads

::: ftmq.aggregate.EntityPayload

::: ftmq.aggregate.merge_schema
