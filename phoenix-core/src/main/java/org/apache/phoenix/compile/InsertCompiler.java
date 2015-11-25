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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.*;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.*;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.*;

import java.io.IOException;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.BitSet;

/**
 * Created by Administrator on 2015/11/10.
 */
public class InsertCompiler {

    private static void setValues(byte[][] values, int[] pkSlotIndex, int[] columnIndexes, PTable table, Map<ImmutableBytesPtr,MutationState.RowMutationState> mutation, PhoenixStatement statement) {
        Map<PColumn,byte[]> columnValues = Maps.newHashMapWithExpectedSize(columnIndexes.length);
        byte[][] pkValues = new byte[table.getPKColumns().size()][];
        // If the table uses salting, the first byte is the salting byte, set to an empty array
        // here and we will fill in the byte later in PRowImpl.
        if (table.getBucketNum() != null) {
            pkValues[0] = new byte[] {0};
        }
        for (int i = 0; i < values.length; i++) {
            byte[] value = values[i];
            PColumn column = table.getColumns().get(columnIndexes[i]);
            if (SchemaUtil.isPKColumn(column)) {
                pkValues[pkSlotIndex[i]] = value;
            } else {
                columnValues.put(column, value);
            }
        }
        ImmutableBytesPtr ptr = new ImmutableBytesPtr();
        table.newKey(ptr, pkValues);
        mutation.put(ptr, new MutationState.RowMutationState(columnValues, statement.getConnection().getStatementExecutionCounter()));
    }
    private final PhoenixStatement statement;
    public InsertCompiler(PhoenixStatement statement){
        this.statement=statement;
    }

    public MutationPlan compile(InsertStatement insert) throws SQLException{
        final PhoenixConnection connection = statement.getConnection();
        ConnectionQueryServices services = connection.getQueryServices();
        final int maxSize = services.getProps().getInt(QueryServices.MAX_MUTATION_SIZE_ATTRIB, QueryServicesOptions.DEFAULT_MAX_MUTATION_SIZE);
        List<ColumnName> columnNodes = insert.getColumns();
        TableRef tableRefToBe = null;
        PTable table = null;
        Set<PColumn> addViewColumnsToBe = Collections.emptySet();
        Set<PColumn> overlapViewColumnsToBe = Collections.emptySet();
        List<PColumn> allColumnsToBe = Collections.emptyList();
        boolean isTenantSpecific = false;
        boolean isSharedViewIndex = false;
        String tenantIdStr = null;
        ColumnResolver resolver = null;
        int[] columnIndexesToBe;
        int nColumnsToSet = 0;
        int[] pkSlotIndexesToBe;
        List<ParseNode> valueNodes = insert.getValues();
        List<PColumn> targetColumns;
        NamedTableNode tableNode = insert.getTable();
        String tableName = tableNode.getName().getTableName();
        String schemaName = tableNode.getName().getSchemaName();
        QueryPlan queryPlanToBe = null;
        int nValuesToSet;
        boolean sameTable = false;
        boolean runOnServer = false;
        // Retry once if auto commit is off, as the meta data may
        // be out of date. We do not retry if auto commit is on, as we
        // update the cache up front when we create the resolver in that case.
        boolean retryOnce = !connection.getAutoCommit();
        while (true) {
            try {
                resolver = FromCompiler.getResolverForMutation(insert, connection);
                tableRefToBe = resolver.getTables().get(0);
                table = tableRefToBe.getTable();
                if (table.getType() == PTableType.VIEW) {
                    if (table.getViewType().isReadOnly()) {
                        throw new ReadOnlyTableException(schemaName,tableName);
                    }
                }
                boolean isSalted = table.getBucketNum() != null;
                isTenantSpecific = table.isMultiTenant() && connection.getTenantId() != null;
                isSharedViewIndex = table.getViewIndexId() != null;
                tenantIdStr = isTenantSpecific ? connection.getTenantId().getString() : null;
                int posOffset = isSalted ? 1 : 0;
                // Setup array of column indexes parallel to values that are going to be set
                allColumnsToBe = table.getColumns();

                nColumnsToSet = 0;
                if (table.getViewType() == PTable.ViewType.UPDATABLE) {
                    addViewColumnsToBe = Sets.newLinkedHashSetWithExpectedSize(allColumnsToBe.size());
                    for (PColumn column : allColumnsToBe) {
                        if (column.getViewConstant() != null) {
                            addViewColumnsToBe.add(column);
                        }
                    }
                }
                ImmutableBytesWritable ptr = new ImmutableBytesWritable();

                // Allow full row insert if no columns or only dynamic ones are specified and values count match
                if (columnNodes.isEmpty()) {
                    nColumnsToSet = allColumnsToBe.size() - posOffset;
                    columnIndexesToBe = new int[nColumnsToSet];
                    pkSlotIndexesToBe = new int[columnIndexesToBe.length];
                    targetColumns = Lists.newArrayListWithExpectedSize(columnIndexesToBe.length);
                    targetColumns.addAll(Collections.<PColumn>nCopies(columnIndexesToBe.length, null));
                    int minPKPos = 0;
                    if (isTenantSpecific) {
                        PColumn tenantColumn = table.getPKColumns().get(minPKPos);
                        columnIndexesToBe[minPKPos] = tenantColumn.getPosition();
                        targetColumns.set(minPKPos, tenantColumn);
                        minPKPos++;
                    }
                    if (isSharedViewIndex) {
                        PColumn indexIdColumn = table.getPKColumns().get(minPKPos);
                        columnIndexesToBe[minPKPos] = indexIdColumn.getPosition();
                        targetColumns.set(minPKPos, indexIdColumn);
                        minPKPos++;
                    }
                    for (int i = posOffset, j = 0; i < allColumnsToBe.size(); i++) {
                        PColumn column = allColumnsToBe.get(i);
                        if (SchemaUtil.isPKColumn(column)) {
                            pkSlotIndexesToBe[i-posOffset] = j + posOffset;
                            if (j++ < minPKPos) { // Skip, as it's already been set above
                                continue;
                            }
                            minPKPos = 0;
                        }
                        columnIndexesToBe[i-posOffset+minPKPos] = i;
                        targetColumns.set(i-posOffset+minPKPos, column);
                    }
                    if (!addViewColumnsToBe.isEmpty()) {
                        // All view columns overlap in this case
                        overlapViewColumnsToBe = addViewColumnsToBe;
                        addViewColumnsToBe = Collections.emptySet();
                    }
                } else {
                    // Size for worse case
                    int numColsInUpsert = columnNodes.size();
                    nColumnsToSet = numColsInUpsert + addViewColumnsToBe.size() + (isTenantSpecific ? 1 : 0) +  + (isSharedViewIndex ? 1 : 0);
                    columnIndexesToBe = new int[nColumnsToSet];
                    pkSlotIndexesToBe = new int[columnIndexesToBe.length];
                    targetColumns = Lists.newArrayListWithExpectedSize(columnIndexesToBe.length);
                    targetColumns.addAll(Collections.<PColumn>nCopies(columnIndexesToBe.length, null));
                    Arrays.fill(columnIndexesToBe, -1); // TODO: necessary? So we'll get an AIOB exception if it's not replaced
                    Arrays.fill(pkSlotIndexesToBe, -1); // TODO: necessary? So we'll get an AIOB exception if it's not replaced
                    BitSet pkColumnsSet = new BitSet(table.getPKColumns().size());
                    int i = 0;
                    // Add tenant column directly, as we don't want to resolve it as this will fail
                    if (isTenantSpecific) {
                        PColumn tenantColumn = table.getPKColumns().get(i + posOffset);
                        columnIndexesToBe[i] = tenantColumn.getPosition();
                        pkColumnsSet.set(pkSlotIndexesToBe[i] = i + posOffset);
                        targetColumns.set(i, tenantColumn);
                        i++;
                    }
                    if (isSharedViewIndex) {
                        PColumn indexIdColumn = table.getPKColumns().get(i + posOffset);
                        columnIndexesToBe[i] = indexIdColumn.getPosition();
                        pkColumnsSet.set(pkSlotIndexesToBe[i] = i + posOffset);
                        targetColumns.set(i, indexIdColumn);
                        i++;
                    }
                    for (ColumnName colName : columnNodes) {
                        ColumnRef ref = resolver.resolveColumn(null, colName.getFamilyName(), colName.getColumnName());
                        PColumn column = ref.getColumn();
                        if (IndexUtil.getViewConstantValue(column, ptr)) {
                            if (overlapViewColumnsToBe.isEmpty()) {
                                overlapViewColumnsToBe = Sets.newHashSetWithExpectedSize(addViewColumnsToBe.size());
                            }
                            nColumnsToSet--;
                            overlapViewColumnsToBe.add(column);
                            addViewColumnsToBe.remove(column);
                        }
                        columnIndexesToBe[i] = ref.getColumnPosition();
                        targetColumns.set(i, column);
                        if (SchemaUtil.isPKColumn(column)) {
                            pkColumnsSet.set(pkSlotIndexesToBe[i] = ref.getPKSlotPosition());
                        }
                        i++;
                    }
                    for (PColumn column : addViewColumnsToBe) {
                        columnIndexesToBe[i] = column.getPosition();
                        targetColumns.set(i, column);
                        if (SchemaUtil.isPKColumn(column)) {
                            pkColumnsSet.set(pkSlotIndexesToBe[i] = SchemaUtil.getPKPosition(table, column));
                        }
                        i++;
                    }
                    for (i = posOffset; i < table.getPKColumns().size(); i++) {
                        PColumn pkCol = table.getPKColumns().get(i);
                        if (!pkColumnsSet.get(i)) {
                            if (!pkCol.isNullable()) {
                                throw new ConstraintViolationException(table.getName().getString() + "." + pkCol.getName().getString() + " may not be null");
                            }
                        }
                    }
                }
                boolean isAutoCommit = connection.getAutoCommit();

                nValuesToSet = valueNodes.size() + addViewColumnsToBe.size() + (isTenantSpecific ? 1 : 0) + (isSharedViewIndex ? 1 : 0);
                // Resize down to allow a subset of columns to be specifiable
                if (columnNodes.isEmpty() && columnIndexesToBe.length >= nValuesToSet) {
                    nColumnsToSet = nValuesToSet;
                    columnIndexesToBe = Arrays.copyOf(columnIndexesToBe, nValuesToSet);
                    pkSlotIndexesToBe = Arrays.copyOf(pkSlotIndexesToBe, nValuesToSet);
                }

                if (nValuesToSet != nColumnsToSet) {
                    // We might have added columns, so refresh cache and try again if stale.
                    // Note that this check is not really sufficient, as a column could have
                    // been removed and the added back and we wouldn't detect that here.
                    if (retryOnce) {
                        retryOnce = false;
                        if (new MetaDataClient(connection).updateCache(schemaName, tableName).wasUpdated()) {
                            continue;
                        }
                    }
                    throw new SQLExceptionInfo.Builder(SQLExceptionCode.INSERT_COLUMN_NUMBERS_MISMATCH)
                            .setMessage("Numbers of columns: " + nColumnsToSet + ". Number of values: " + nValuesToSet)
                            .build().buildException();
                }
            } catch (MetaDataEntityNotFoundException e) {
                // Catch column/column family not found exception, as our meta data may
                // be out of sync. Update the cache once and retry if we were out of sync.
                // Otherwise throw, as we'll just get the same error next time.
                if (retryOnce) {
                    retryOnce = false;
                    if (new MetaDataClient(connection).updateCache(schemaName, tableName).wasUpdated()) {
                        continue;
                    }
                }
                throw e;
            }
            break;
        }

        RowProjector projectorToBe = null;
        final List<PColumn> allColumns = allColumnsToBe;
        final RowProjector projector = projectorToBe;
        final QueryPlan queryPlan = queryPlanToBe;
        final TableRef tableRef = tableRefToBe;
        final int[] columnIndexes = columnIndexesToBe;
        final int[] pkSlotIndexes = pkSlotIndexesToBe;
        final Set<PColumn> addViewColumns = addViewColumnsToBe;
        final Set<PColumn> overlapViewColumns = overlapViewColumnsToBe;


        ////////////////////////////////////////////////////////////////////
        // INSERT VALUES
        /////////////////////////////////////////////////////////////////////
        int nodeIndex = 0;
        // initialze values with constant byte values first
        final byte[][] values = new byte[nValuesToSet][];
        if (isTenantSpecific) {
            PName tenantId = connection.getTenantId();
            tenantId = ScanUtil.padTenantIdIfNecessary(table.getRowKeySchema(), table.getBucketNum() != null, tenantId);
            values[nodeIndex++] = connection.getTenantId().getBytes();
        }
        if (isSharedViewIndex) {
            values[nodeIndex++] = MetaDataUtil.getViewIndexIdDataType().toBytes(table.getViewIndexId());
        }
        final int nodeIndexOffset = nodeIndex;
        // Allocate array based on size of all columns in table,
        // since some values may not be set (if they're nullable).
        final StatementContext context = new StatementContext(statement, resolver, new Scan(), new SequenceManager(statement));
        InsertValuesCompiler expressionBuilder = new InsertValuesCompiler(context);
        final List<Expression> constantExpressions = Lists.newArrayListWithExpectedSize(valueNodes.size());
        // First build all the expressions, as with sequences we want to collect them all first
        // and initialize them in one batch
        Map<PColumn,Object> pkMap=new HashMap<PColumn,Object>();
        for (ParseNode valueNode : valueNodes) {
            if (!valueNode.isStateless()) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.VALUE_IN_INSERT_NOT_CONSTANT).build().buildException();
            }
            PColumn column = allColumns.get(columnIndexes[nodeIndex]);
            expressionBuilder.setColumn(column);
            Expression expression = valueNode.accept(expressionBuilder);
            if (expression.getDataType() != null && !expression.getDataType().isCastableTo(column.getDataType())) {
                throw TypeMismatchException.newException(
                        expression.getDataType(), column.getDataType(), "expression: "
                                + expression.toString() + " in column " + column);
            }
            constantExpressions.add(expression);
            if(SchemaUtil.isPKColumn(column)){
                ImmutableBytesWritable ptr = context.getTempPtr();
                final SequenceManager sequenceManager = context.getSequenceManager();
                Tuple tuple = sequenceManager.getSequenceCount() == 0 ? null :
                        sequenceManager.newSequenceTuple(null);
                expression.evaluate(tuple, ptr);
                Object value=null;
                value = expression.getDataType().toObject(context.getTempPtr(), expression.getSortOrder(), expression.getMaxLength(), expression.getScale());
                pkMap.put(column, value);
            }
            nodeIndex++;
        }

        return new MutationPlan() {

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
                ImmutableBytesWritable ptr = context.getTempPtr();
                final SequenceManager sequenceManager = context.getSequenceManager();
                // Next evaluate all the expressions
                int nodeIndex = nodeIndexOffset;
                PTable table = tableRef.getTable();
                Tuple tuple = sequenceManager.getSequenceCount() == 0 ? null :
                        sequenceManager.newSequenceTuple(null);
                for (Expression constantExpression : constantExpressions) {
                    PColumn column = allColumns.get(columnIndexes[nodeIndex]);
                    constantExpression.evaluate(tuple, ptr);
                    Object value = null;
                    if (constantExpression.getDataType() != null) {
                        value = constantExpression.getDataType().toObject(ptr, constantExpression.getSortOrder(), constantExpression.getMaxLength(), constantExpression.getScale());
                        if (!constantExpression.getDataType().isCoercibleTo(column.getDataType(), value)) {
                            throw TypeMismatchException.newException(
                                    constantExpression.getDataType(), column.getDataType(), "expression: "
                                            + constantExpression.toString() + " in column " + column);
                        }
                        if (!column.getDataType().isSizeCompatible(ptr, value, constantExpression.getDataType(),
                                constantExpression.getMaxLength(), constantExpression.getScale(),
                                column.getMaxLength(), column.getScale())) {
                            throw new SQLExceptionInfo.Builder(
                                    SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY).setColumnName(column.getName().getString())
                                    .setMessage("value=" + constantExpression.toString()).build().buildException();
                        }
                    }
                    column.getDataType().coerceBytes(ptr, value, constantExpression.getDataType(),
                            constantExpression.getMaxLength(), constantExpression.getScale(), constantExpression.getSortOrder(),
                            column.getMaxLength(), column.getScale(),column.getSortOrder(),
                            table.rowKeyOrderOptimizable());
                    if (overlapViewColumns.contains(column) && Bytes.compareTo(ptr.get(), ptr.getOffset(), ptr.getLength(), column.getViewConstant(), 0, column.getViewConstant().length - 1) != 0) {
                        throw new SQLExceptionInfo.Builder(
                                SQLExceptionCode.CANNOT_UPDATE_VIEW_COLUMN)
                                .setColumnName(column.getName().getString())
                                .setMessage("value=" + constantExpression.toString()).build().buildException();
                    }
                    values[nodeIndex] = ByteUtil.copyKeyBytesIfNecessary(ptr);
                    nodeIndex++;
                }
                // Add columns based on view
                for (PColumn column : addViewColumns) {
                    if (IndexUtil.getViewConstantValue(column, ptr)) {
                        values[nodeIndex++] = ByteUtil.copyKeyBytesIfNecessary(ptr);
                    } else {
                        throw new IllegalStateException();
                    }
                }
                Map<ImmutableBytesPtr, MutationState.RowMutationState> mutation = Maps.newHashMapWithExpectedSize(1);
                setValues(values, pkSlotIndexes, columnIndexes, table, mutation, statement);
                byte[] bytes=null;
                for(ImmutableBytesPtr p:mutation.keySet()){
                    bytes=p.copyBytesIfNecessary();
                }
                HTableInterface htable=connection.getQueryServices().getTable(table.getTableName().getBytes());

                /**
                 * check if the data exists
                 */
                Get get=new Get(bytes);
                try {
                    if(htable.exists(get)){
                        throw new SQLException("primary key is exists","Phoenix Exception");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                }
                return new MutationState(tableRef, mutation, 0, maxSize, connection);
            }

            @Override
            public ExplainPlan getExplainPlan() throws SQLException {
                List<String> planSteps = Lists.newArrayListWithExpectedSize(2);
                if (context.getSequenceManager().getSequenceCount() > 0) {
                    planSteps.add("CLIENT RESERVE " + context.getSequenceManager().getSequenceCount() + " SEQUENCES");
                }
                planSteps.add("PUT SINGLE ROW");
                return new ExplainPlan(planSteps);
            }

        };
    }

    private static final class InsertValuesCompiler extends ExpressionCompiler {
        private PColumn column;

        private InsertValuesCompiler(StatementContext context) {
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
