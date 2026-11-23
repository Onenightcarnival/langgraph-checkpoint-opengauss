from langgraph.store.postgres import base

from .._session import load_queries

_QUERIES = load_queries(__package__)
_MIGRATION = _QUERIES["migration"]
_PUT = _QUERIES["put"]


def _upsert(query: str, clause: str) -> str:
    insert, separator, _ = query.partition(_PUT["conflict"])
    return insert + clause if separator else query


def _vector_migrations(store):
    config = store.index_config
    dims = int(config["dims"])
    if dims < 1:
        raise ValueError("dims must be positive")
    ops = base._get_vector_type_ops(store)
    vector_type = config.get("ann_index_config", {}).get("vector_type", "vector")
    kind, params = base._get_index_params(store)
    yield _MIGRATION["vector_type"]
    yield base.VECTOR_MIGRATIONS[1].sql % {"dims": dims, "vector_type": vector_type}
    if kind != "flat":
        options = ", ".join(f"{key}={value}" for key, value in params.items())
        yield _MIGRATION["vector_index"].format(kind=kind, ops=ops, options=f" WITH ({options})" if options else "")


def _migrate(table, statements):
    yield _MIGRATION["create"].format(table=table), (), False
    rows = yield _MIGRATION["latest"].format(table=table), (), True
    latest = rows[0]["version"]
    for version, statement in enumerate(statements):
        if latest is None or version > latest:
            yield statement, (), False
            yield _MIGRATION["record"].format(table=table), (version,), False


class StoreSQL:
    MIGRATIONS = (*[s.replace("INDEX CONCURRENTLY", "INDEX") for s in base.MIGRATIONS], _MIGRATION["fractional_ttl"])
    SWEEP_SQL = _QUERIES["ttl"]["sweep"]

    def migrations(self):
        yield _MIGRATION["lock"], (), False
        yield from _migrate("store_migrations", self.MIGRATIONS)
        if self.index_config:
            yield from _migrate("vector_migrations", _vector_migrations(self))

    def _prepare_batch_PUT_queries(self, put_ops):
        queries, embedding = super()._prepare_batch_PUT_queries(put_ops)
        items = [(_upsert(query, _PUT["item"]), params) for query, params in queries]
        if self.index_config and put_ops:
            keys = {(base._namespace_to_text(op.namespace), op.key) for _, op in put_ops if op.value is not None}
            if keys:
                prefixes, item_keys = zip(*sorted(keys), strict=True)
                items.append((_PUT["clear_vectors"], (list(prefixes), list(item_keys))))
        if embedding:
            query, params = embedding
            embedding = _upsert(query, _PUT["vector"]), params
        return items, embedding

    def _search_pages(self, ops, results, queries, requests, vectors):
        by_query = {idx: vector for (idx, _), vector in zip(requests, vectors, strict=True)}
        for idx, ((result_idx, op), (query, params)) in enumerate(zip(ops, queries, strict=True)):
            if idx in by_query:
                params = [by_query[idx] if value is base.PLACEHOLDER else value for value in params]
                # The final three bindings are the candidate limit, item limit and item offset.
                params[-3] += op.offset * self.index_config["__estimated_num_vectors"]
            rows = yield query, params, True
            if idx in by_query and (rows or op.offset) and len(rows) < op.limit:
                # Several nearest fields may belong to one item. Rank all matches for a short page.
                rows = yield query, [*params[:-3], None, op.limit, op.offset], True
            results[result_idx] = [
                base._row_to_search_item(base._decode_ns_bytes(row["prefix"]), row, loader=self._deserializer)
                for row in rows
            ]
