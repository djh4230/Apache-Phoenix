/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.compile;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sun.istack.NotNull;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.BaseQueryPlan;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.execute.MutationState.RowMutationState;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.optimize.QueryOptimizer;
import org.apache.phoenix.parse.*;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.*;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SchemaUtil;

import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class UpdateCompiler {
    private static ParseNodeFactory FACTORY = new ParseNodeFactory();

    private final PhoenixStatement statement;

    public UpdateCompiler(PhoenixStatement statement) {
        this.statement = statement;
    }

    private static void setValues(byte[][] pkValues, Map<PColumn,byte[]> setsValues, PTable table, Map<ImmutableBytesPtr,RowMutationState> mutation, PhoenixStatement statement) {
        ImmutableBytesPtr ptr = new ImmutableBytesPtr();
        table.newKey(ptr, pkValues);
        mutation.put(ptr, new RowMutationState(setsValues, statement.getConnection().getStatementExecutionCounter()));
    }

    private static MutationState updateRows(StatementContext childContext, TableRef tableRef, ResultIterator iterator, RowProjector projector,Map<PColumn,byte[]> setsValues) throws SQLException {
        PhoenixStatement statement = childContext.getStatement();
        PhoenixConnection connection = statement.getConnection();
        ConnectionQueryServices services = connection.getQueryServices();
        int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,
                QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
        int batchSize = Math.min(connection.getMutateBatchSize(), maxSize);
        boolean isAutoCommit = connection.getAutoCommit();

        int rowCount = 0;
        Map<ImmutableBytesPtr, RowMutationState> mutation = Maps.newHashMapWithExpectedSize(batchSize);
        PTable table = tableRef.getTable();
        byte[][] PkValues = new byte[table.getPKColumns().size()][];
        if (table.getBucketNum() != null) {
            PkValues[0] = new byte[] {0};
        }

        try (ResultSet rs = new PhoenixResultSet(iterator, projector, childContext)) {
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            while (rs.next()) {
                int k=0;
                for (int i = 0; i < PkValues.length; i++,k++) {
                    PColumn column = table.getColumns().get(i);
                    if(SchemaUtil.isPKColumn(column)){
                        byte[] bytes = rs.getBytes(i + 1);
                        ptr.set(bytes == null ? ByteUtil.EMPTY_BYTE_ARRAY : bytes);
                        Object value = rs.getObject(i + 1);
                        int rsPrecision = rs.getMetaData().getPrecision(i + 1);
                        Integer precision = rsPrecision == 0 ? null : rsPrecision;
                        int rsScale = rs.getMetaData().getScale(i + 1);
                        Integer scale = rsScale == 0 ? null : rsScale;
                        // We are guaranteed that the two column will have compatible types,
                        // as we checked that before.
                        if (!column.getDataType().isSizeCompatible(ptr, value, column.getDataType(), precision, scale,
                                column.getMaxLength(), column.getScale())) { throw new SQLExceptionInfo.Builder(
                                SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY).setColumnName(column.getName().getString())
                                .setMessage("value=" + column.getDataType().toStringLiteral(ptr, null)).build()
                                .buildException(); }
                        column.getDataType().coerceBytes(ptr, value, column.getDataType(),
                                precision, scale, SortOrder.getDefault(),
                                column.getMaxLength(), column.getScale(), column.getSortOrder(),
                                table.rowKeyOrderOptimizable());
                        if(table.getBucketNum() != null){
                            k=i+1;
                        }
                        PkValues[k] = ByteUtil.copyKeyBytesIfNecessary(ptr);
                    }
                }
                setValues(PkValues,setsValues ,table, mutation, statement);
                rowCount++;
                // Commit a batch if auto commit is true and we're at our batch size
                if (isAutoCommit && rowCount % batchSize == 0) {
                    MutationState state = new MutationState(tableRef, mutation, 0, maxSize, connection);
                    connection.getMutationState().join(state);
                    connection.commit();
                    mutation.clear();
                }
            }
            // If auto commit is true, this last batch will be committed upon return
            return new MutationState(tableRef, mutation, rowCount / batchSize * batchSize, maxSize, connection);
        }
    }

    private Set<PTable> getNonDisabledImmutableIndexes(TableRef tableRef) {
        PTable table = tableRef.getTable();
        if (table.isImmutableRows() && !table.getIndexes().isEmpty()) {
            Set<PTable> nonDisabledIndexes = Sets.newHashSetWithExpectedSize(table.getIndexes().size());
            for (PTable index : table.getIndexes()) {
                if (index.getIndexState() != PIndexState.DISABLE) {
                    nonDisabledIndexes.add(index);
                }
            }
            return nonDisabledIndexes;
        }
        return Collections.emptySet();
    }

    private class MultiDeleteMutationPlan implements MutationPlan {
        private final List<MutationPlan> plans;
        private final MutationPlan firstPlan;

        public MultiDeleteMutationPlan(@NotNull List<MutationPlan> plans) {
            Preconditions.checkArgument(!plans.isEmpty());
            this.plans = plans;
            this.firstPlan = plans.get(0);
        }

        @Override
        public StatementContext getContext() {
            return firstPlan.getContext();
        }

        @Override
        public ParameterMetaData getParameterMetaData() {
            return firstPlan.getParameterMetaData();
        }

        @Override
        public ExplainPlan getExplainPlan() throws SQLException {
            return firstPlan.getExplainPlan();
        }

        @Override
        public PhoenixConnection getConnection() {
            return firstPlan.getConnection();
        }

        @Override
        public MutationState execute() throws SQLException {
            MutationState state = firstPlan.execute();
            for (MutationPlan plan : plans.subList(1, plans.size())) {
                plan.execute();
            }
            return state;
        }
    }

    public MutationPlan compile(UpdateStatement update) throws SQLException {
        final PhoenixConnection connection = statement.getConnection();
        final boolean isAutoCommit = connection.getAutoCommit();
        final boolean hasLimit = false;
        final ConnectionQueryServices services = connection.getQueryServices();
        List<QueryPlan> queryPlans;
        NamedTableNode tableNode = update.getTable();
        String tableName = tableNode.getName().getTableName();
        String schemaName = tableNode.getName().getSchemaName();
        boolean retryOnce = !isAutoCommit;
        TableRef tableRefToBe;
        boolean noQueryReqd = false;
        boolean runOnServer = false;
        SelectStatement select = null;
        Set<PTable> immutableIndex = Collections.emptySet();
        //UpdateParallelIteratorFactory parallelIteratorFactory = null;
        ParseNode where=update.getWhere();
        List<ParseNode> parseNodes=update.getValues();
        final Map<PColumn,byte[]> setsValue=new HashedMap();

        while (true) {
            try {
                ColumnResolver resolver = FromCompiler.getResolverForMutation(update, connection);
                tableRefToBe = resolver.getTables().get(0);
                PTable table = tableRefToBe.getTable();

                final StatementContext cont = new StatementContext(statement, resolver, new Scan(), new SequenceManager(statement));
                UpdateValuesCompiler expressionBuilder = new UpdateValuesCompiler(cont);

                for(ParseNode node:parseNodes){
                    String key=node.getChildren().get(0).toString();
                    PColumn pColumn=table.getColumn(key.substring(1,key.length()-1));
                    if(SchemaUtil.isPKColumn(pColumn)){
                        throw new SQLException("can not update primary key","Phoenix Exception");
                    }
                    expressionBuilder.setColumn(pColumn);
                    ParseNode valueNode=node.getChildren().get(1);
                    Expression expression = valueNode.accept(expressionBuilder);
                    ImmutableBytesWritable ptr = cont.getTempPtr();
                    final SequenceManager sequenceManager = cont.getSequenceManager();
                    Tuple tuple = sequenceManager.getSequenceCount() == 0 ? null :sequenceManager.newSequenceTuple(null);
                    expression.evaluate(tuple, ptr);
                    Object value=null;
                    value = expression.getDataType().toObject(cont.getTempPtr(), expression.getSortOrder(), expression.getMaxLength(), expression.getScale());
                    if (!expression.getDataType().isCoercibleTo(pColumn.getDataType(), value)) {
                        throw TypeMismatchException.newException(
                                expression.getDataType(), pColumn.getDataType(), "expression: "
                                        + expression.toString() + " in column " + pColumn);
                    }
                    if (!pColumn.getDataType().isSizeCompatible(ptr, value, expression.getDataType(),
                            expression.getMaxLength(), expression.getScale(),
                            pColumn.getMaxLength(), pColumn.getScale())) {
                        throw new SQLExceptionInfo.Builder(
                                SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY).setColumnName(pColumn.getName().getString())
                                .setMessage("value=" + expression.toString()).build().buildException();
                    }

                pColumn.getDataType().coerceBytes(ptr, value, expression.getDataType(),
                        expression.getMaxLength(), expression.getScale(), expression.getSortOrder(),
                        pColumn.getMaxLength(), pColumn.getScale(),pColumn.getSortOrder(),
                        table.rowKeyOrderOptimizable());
                    setsValue.put(pColumn,ByteUtil.copyKeyBytesIfNecessary(ptr));
                   // }
                }


                if (table.getType() == PTableType.VIEW && table.getViewType().isReadOnly()) {
                    throw new ReadOnlyTableException(table.getSchemaName().getString(),table.getTableName().getString());
                }

                immutableIndex = getNonDisabledImmutableIndexes(tableRefToBe);
                boolean mayHaveImmutableIndexes = !immutableIndex.isEmpty();
                noQueryReqd = !hasLimit;
                runOnServer = isAutoCommit && noQueryReqd;
                HintNode hint = update.getHint();
                if (runOnServer && !update.getHint().hasHint(Hint.USE_INDEX_OVER_DATA_TABLE)) {
                    hint = HintNode.create(hint, Hint.USE_DATA_OVER_INDEX_TABLE);
                }

                List<AliasedNode> aliasedNodes = Lists.newArrayListWithExpectedSize(table.getPKColumns().size());
                boolean isSalted = table.getBucketNum() != null;
                boolean isMultiTenant = connection.getTenantId() != null && table.isMultiTenant();
                boolean isSharedViewIndex = table.getViewIndexId() != null;
                for (int i = (isSalted ? 1 : 0) + (isMultiTenant ? 1 : 0) + (isSharedViewIndex ? 1 : 0); i < table.getPKColumns().size(); i++) {
                    PColumn column = table.getPKColumns().get(i);
                    aliasedNodes.add(FACTORY.aliasedNode(null, FACTORY.column(null, '"' + column.getName().getString() + '"', null)));
                }
                select = FACTORY.select(
                        update.getTable(),
                        hint, false, aliasedNodes, where,
                        Collections.<ParseNode>emptyList(), null,
                        Collections.<OrderByNode>emptyList(), null,
                        update.getBindCount(), false, false, Collections.<SelectStatement>emptyList(), update.getUdfParseNodes());
                select = StatementNormalizer.normalize(select, resolver);
                SelectStatement transformedSelect = SubqueryRewriter.transform(select, resolver, connection);
                if (transformedSelect != select) {
                    resolver = FromCompiler.getResolverForQuery(transformedSelect, connection);
                    select = StatementNormalizer.normalize(transformedSelect, resolver);
                }
                //parallelIteratorFactory = new UpdateParallelIteratorFactory(connection);
                QueryOptimizer optimizer = new QueryOptimizer(services);
                queryPlans = Lists.newArrayList(mayHaveImmutableIndexes
                        ? optimizer.getApplicablePlans(statement, select, resolver, Collections.<PColumn>emptyList(), null)
                        : optimizer.getBestPlan(statement, select, resolver, Collections.<PColumn>emptyList(), null));
                if (mayHaveImmutableIndexes) { // FIXME: this is ugly
                    // Lookup the table being deleted from in the cache, as it's possible that the
                    // optimizer updated the cache if it found indexes that were out of date.
                    // If the index was marked as disabled, it should not be in the list
                    // of immutable indexes.
                    table = connection.getMetaDataCache().getTable(new PTableKey(table.getTenantId(), table.getName().getString()));
                    tableRefToBe.setTable(table);
                    immutableIndex = getNonDisabledImmutableIndexes(tableRefToBe);
                }
            } catch (MetaDataEntityNotFoundException e) {
                // Catch column/column family not found exception, as our meta data may
                // be out of sync. Update the cache once and retry if we were out of sync.
                // Otherwise throw, as we'll just get the same error next time.
                if (retryOnce) {
                    retryOnce = false;
                    MetaDataMutationResult result = new MetaDataClient(connection).updateCache(schemaName, tableName);
                    if (result.wasUpdated()) {
                        continue;
                    }
                }
                throw e;
            }
            break;
        }
        final boolean hasImmutableIndexes = !immutableIndex.isEmpty();
        // tableRefs is parallel with queryPlans
        TableRef[] tableRefs = new TableRef[hasImmutableIndexes ? immutableIndex.size() : 1];
        if (hasImmutableIndexes) {
            int i = 0;
            Iterator<QueryPlan> plans = queryPlans.iterator();
            while (plans.hasNext()) {
                QueryPlan plan = plans.next();
                PTable table = plan.getTableRef().getTable();
                if (table.getType() == PTableType.INDEX) { // index plans
                    tableRefs[i++] = plan.getTableRef();
                    immutableIndex.remove(table);
                } else { // data plan
                    /*
                     * If we have immutable indexes that we need to maintain, don't execute the data plan
                     * as we can save a query by piggy-backing on any of the other index queries, since the
                     * PK columns that we need are always in each index row.
                     */
                    plans.remove();
                }
            }
            /*
             * If we have any immutable indexes remaining, then that means that the plan for that index got filtered out
             * because it could not be executed. This would occur if a column in the where clause is not found in the
             * immutable index.
             */
            if (!immutableIndex.isEmpty()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.INVALID_FILTER_ON_IMMUTABLE_ROWS).setSchemaName(tableRefToBe.getTable().getSchemaName().getString())
                        .setTableName(tableRefToBe.getTable().getTableName().getString()).build().buildException();
            }
        }

        // Make sure the first plan is targeting deletion from the data table
        // In the case of an immutable index, we'll also delete from the index.
        tableRefs[0] = tableRefToBe;
        /*
         * Create a mutationPlan for each queryPlan. One plan will be for the deletion of the rows
         * from the data table, while the others will be for deleting rows from immutable indexes.
         */
        List<MutationPlan> mutationPlans = Lists.newArrayListWithExpectedSize(tableRefs.length);
        for (int i = 0; i < tableRefs.length; i++) {
            final TableRef tableRef = tableRefs[i];
            final QueryPlan plan = queryPlans.get(i);
            if (!plan.getTableRef().equals(tableRef) || !(plan instanceof BaseQueryPlan)) {
                runOnServer = false;
                noQueryReqd = false; // FIXME: why set this to false in this case?
            }

            final int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB,QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);

            final StatementContext context = plan.getContext();
            // If we're doing a query for a set of rows with no where clause, then we don't need to contact the server at all.
            // A simple check of the none existence of a where clause in the parse node is not sufficient, as the where clause
            // may have been optimized out. Instead, we check that there's a single SkipScanFilter
                mutationPlans.add( new MutationPlan() {

                    @Override
                    public PhoenixConnection getConnection() {
                        return connection;
                    }

                    @Override
                    public ParameterMetaData getParameterMetaData() {
                        return context.getBindManager().getParameterMetaData();
                    }

                    @Override
                    public StatementContext getContext() {
                        return context;
                    }

                    @Override
                    public MutationState execute() throws SQLException {
                        ResultIterator iterator = plan.iterator();
                        return updateRows(plan.getContext(), tableRef, iterator,plan.getProjector() , setsValue);
                    }

                    @Override
                    public ExplainPlan getExplainPlan() throws SQLException {
                        List<String> queryPlanSteps =  plan.getExplainPlan().getPlanSteps();
                        List<String> planSteps = Lists.newArrayListWithExpectedSize(queryPlanSteps.size()+1);
                        planSteps.add("UPDATE ROWS");
                        planSteps.addAll(queryPlanSteps);
                        return new ExplainPlan(planSteps);
                    }
                });
            }
        return mutationPlans.size() == 1 ? mutationPlans.get(0) : new MultiDeleteMutationPlan(mutationPlans);
    }

    private static final class UpdateValuesCompiler extends ExpressionCompiler {
        private PColumn column;

        private UpdateValuesCompiler(StatementContext context) {
            super(context);
        }

        public void setColumn(PColumn column) {
            this.column = column;
        }

        @Override
        public Expression visit(BindParseNode node) throws SQLException {
            if (isTopLevel()) {
                context.getBindManager().addParamMetaData(node, column);
                Object value = context.getBindManager().getBindValue(node);
                return LiteralExpression.newConstant(value, column.getDataType(), column.getSortOrder(), Determinism.ALWAYS);
            }
            return super.visit(node);
        }

        @Override
        public Expression visit(LiteralParseNode node) throws SQLException {
            if (isTopLevel()) {
                return LiteralExpression.newConstant(node.getValue(), column.getDataType(), column.getSortOrder(), Determinism.ALWAYS);
            }
            return super.visit(node);
        }


        @Override
        public Expression visit(SequenceValueParseNode node) throws SQLException {
            return context.getSequenceManager().newSequenceReference(node);
        }
    }
}
