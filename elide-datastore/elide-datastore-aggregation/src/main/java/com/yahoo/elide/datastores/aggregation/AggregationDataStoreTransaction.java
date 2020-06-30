/*
 * Copyright 2019, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.datastores.aggregation;

import com.yahoo.elide.core.DataStoreTransactionImplementation;
import com.yahoo.elide.core.QueryDetail;
import com.yahoo.elide.core.RequestScope;
import com.yahoo.elide.core.exceptions.InvalidPredicateException;
import com.yahoo.elide.datastores.aggregation.cache.Cache;
import com.yahoo.elide.datastores.aggregation.cache.QueryKeyExtractor;
import com.yahoo.elide.datastores.aggregation.metadata.models.Table;
import com.yahoo.elide.datastores.aggregation.query.ColumnProjection;
import com.yahoo.elide.datastores.aggregation.query.Query;
import com.yahoo.elide.datastores.aggregation.query.QueryResult;
import com.yahoo.elide.datastores.aggregation.query.TimeDimensionProjection;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.SQLQueryEngine;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.metadata.SQLMetric;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.metric.SQLMetricFunction;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.query.SQLQuery;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.query.SQLQueryConstructor;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.query.SQLQueryTemplate;
import com.yahoo.elide.request.EntityProjection;

import com.google.common.annotations.VisibleForTesting;

import com.yahoo.elide.request.Relationship;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Transaction handler for {@link AggregationDataStore}.
 */
@Slf4j
@ToString
public class AggregationDataStoreTransaction extends DataStoreTransactionImplementation {
    private final QueryEngine queryEngine;
    private final Cache cache;
    private final QueryEngine.Transaction queryEngineTransaction;

    @Getter
    private List<String> queryText;

    public AggregationDataStoreTransaction(QueryEngine queryEngine, Cache cache) {
        this.queryEngine = queryEngine;
        this.cache = cache;
        this.queryEngineTransaction = queryEngine.beginTransaction();
        this.queryText = new ArrayList<>();
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

//        //using reflection API to get the SQL query
//        Class c = null;
//        try {
//            c = Class.forName("com.yahoo.elide.datastores.aggregation.queryengines.sql.SQLQueryEngine");
//            SQLQueryEngine sqlQueryEngine = new SQLQueryEngine();
//            Method m = c.getDeclaredMethod("toSQL", Query.class);
//            m.setAccessible(true);
//            SQLQuery myQuery = (SQLQuery)m.invoke(sqlQueryEngine, query);
//            String queryString = myQuery.toString();
//            queryText.add(queryString);
//        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
//            e.printStackTrace();
//        }
        SQLQuery myQuery = queryEngine.toSQL(query);
        String queryString = myQuery.toString();
        queryText.add(queryString);
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
        Class<?> entityClass = projection.getType();
        String modelName = entityClass.getName();
        List<String> queryTextList = getQueryText();
        this.queryText = new ArrayList<>();
        return new QueryDetail(modelName, queryTextList);
    }

    @Override
    public QueryDetail explain(Relationship relationship, RequestScope scope, Object entity) {
        Class<?> entityClass = relationship.getProjection().getType();
        String modelName = entityClass.getName();
        List<String> queryTextList = getQueryText();
        this.queryText = new ArrayList<>();
        return new QueryDetail(modelName, queryTextList);
    }

//    private String toSQL(Query query) {
//        Set<ColumnProjection> groupByDimensions = new LinkedHashSet<>(query.getGroupByDimensions());
//        Set<TimeDimensionProjection> timeDimensions = new LinkedHashSet<>(query.getTimeDimensions());
//
//        SQLQueryTemplate queryTemplate = query.getMetrics().stream()
//                .map(metricProjection -> {
//                    if (!(metricProjection.getColumn().getMetricFunction() instanceof SQLMetricFunction)) {
//                        throw new InvalidPredicateException(
//                                "Non-SQL metric function on " + metricProjection.getAlias());
//                    }
//
//                    return ((SQLMetric) metricProjection.getColumn()).resolve(query, metricProjection, referenceTable);
//                })
//                .reduce(SQLQueryTemplate::merge)
//                .orElse(new SQLQueryTemplate(query));
//
//        return new SQLQueryConstructor(referenceTable).resolveTemplate(
//                query,
//                queryTemplate,
//                query.getSorting(),
//                query.getWhereFilter(),
//                query.getHavingFilter());
//    }
}
