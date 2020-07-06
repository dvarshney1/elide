/*
 * Copyright 2019, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.datastores.aggregation;

import com.yahoo.elide.core.DataStoreTransactionImplementation;
import com.yahoo.elide.core.QueryDetail;
import com.yahoo.elide.core.RequestScope;
import com.yahoo.elide.datastores.aggregation.cache.Cache;
import com.yahoo.elide.datastores.aggregation.cache.QueryKeyExtractor;
import com.yahoo.elide.datastores.aggregation.metadata.models.Table;
import com.yahoo.elide.datastores.aggregation.query.Query;
import com.yahoo.elide.datastores.aggregation.query.QueryResult;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.query.SQLQuery;
import com.yahoo.elide.request.EntityProjection;

import com.google.common.annotations.VisibleForTesting;

import com.yahoo.elide.request.Relationship;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Transaction handler for {@link AggregationDataStore}.
 */
@Slf4j
@ToString
public class AggregationDataStoreTransaction extends DataStoreTransactionImplementation {
    private final QueryEngine queryEngine;
    private final Cache cache;
    private final QueryEngine.Transaction queryEngineTransaction;

    public AggregationDataStoreTransaction(QueryEngine queryEngine, Cache cache) {
        this.queryEngine = queryEngine;
        this.cache = cache;
        this.queryEngineTransaction = queryEngine.beginTransaction();
    }

    @Override
    public void save(Object entity, RequestScope scope) {

    }

    @Override
    public void delete(Object entity, RequestScope scope) {

    }

    @Override
    public void flush(RequestScope scope) {

    }

    @Override
    public void commit(RequestScope scope) {
        queryEngineTransaction.close();
    }

    @Override
    public void createObject(Object entity, RequestScope scope) {

    }

    @Override
    public Iterable<Object> loadObjects(EntityProjection entityProjection, RequestScope scope) {
        Query query = buildQuery(entityProjection, scope);
        QueryResult result = null;

        String cacheKey = null;
        if (cache != null && !query.isBypassingCache()) {
            String tableVersion = queryEngine.getTableVersion(query.getTable(), queryEngineTransaction);
            if (tableVersion != null) {
                cacheKey = tableVersion + ';' + QueryKeyExtractor.extractKey(query);
                result = cache.get(cacheKey);
            }
        }
        if (result == null) {
            result = queryEngine.executeQuery(query, queryEngineTransaction);
            if (cacheKey != null) {
                cache.put(cacheKey, result);
            }
        }
        if (entityProjection.getPagination() != null && entityProjection.getPagination().returnPageTotals()) {
            entityProjection.getPagination().setPageTotals(result.getPageTotals());
        }
        return result.getData();
    }

    @Override
    public void close() throws IOException {
        queryEngineTransaction.close();
    }

    @VisibleForTesting
    Query buildQuery(EntityProjection entityProjection, RequestScope scope) {
        Table table = queryEngine.getTable(scope.getDictionary().getJsonAliasFor(entityProjection.getType()));
        EntityProjectionTranslator translator = new EntityProjectionTranslator(
                queryEngine,
                table,
                entityProjection,
                scope.getDictionary());
        return translator.getQuery();
    }

    @Override
    public void cancel() {
        queryEngineTransaction.cancel();
    }

    @Override
    public QueryDetail explain(EntityProjection projection, RequestScope scope) {
        Query query = buildQuery(projection, scope);
        SQLQuery myQuery = queryEngine.toSQL(query);
        List<String> queryTextList = new ArrayList<>();
        queryTextList.add(myQuery.toString());
        return new QueryDetail(projection.getType().getName(), queryTextList);
    }

    @Override
    public QueryDetail explain(Relationship relationship, RequestScope scope, Object entity) {
        EntityProjection projection = relationship.getProjection();
        Query query = buildQuery(projection, scope);
        SQLQuery myQuery = queryEngine.toSQL(query);
        List<String> queryTextList = new ArrayList<>();
        queryTextList.add(myQuery.toString());
        return new QueryDetail(relationship.getName(), queryTextList);
    }
}
