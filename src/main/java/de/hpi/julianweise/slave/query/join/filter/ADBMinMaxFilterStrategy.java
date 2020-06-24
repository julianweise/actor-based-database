package de.hpi.julianweise.slave.query.join.filter;

import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.query.join.ADBJoinQuery;
import de.hpi.julianweise.query.join.ADBJoinQueryPredicate;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.node.ADBPartitionJoinTask;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import lombok.val;

import java.util.Arrays;
import java.util.List;

public class ADBMinMaxFilterStrategy implements ADBJoinPartitionFilterStrategy {

    private static final List<ADBQueryTerm.RelationalOperator> LESS_OPERATORS =
            Arrays.asList(ADBQueryTerm.RelationalOperator.LESS, ADBQueryTerm.RelationalOperator.LESS_OR_EQUAL);

    private static final List<ADBQueryTerm.RelationalOperator> GREATER_OPERATORS =
            Arrays.asList(ADBQueryTerm.RelationalOperator.GREATER, ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL);

    private final ADBPartitionJoinTask joinTask;
    private final Object2ObjectMap<String, ADBEntityEntry> minValuesLeft = new Object2ObjectOpenHashMap<>();
    private final Object2ObjectMap<String, ADBEntityEntry> maxValuesLeft = new Object2ObjectOpenHashMap<>();
    private final Object2ObjectMap<String, ADBEntityEntry> minValuesRight = new Object2ObjectOpenHashMap<>();
    private final Object2ObjectMap<String, ADBEntityEntry> maxValuesRight = new Object2ObjectOpenHashMap<>();

    public ADBMinMaxFilterStrategy(ADBPartitionJoinTask joinTask, ADBJoinQuery query) {
        this.joinTask = joinTask;
        this.initialize(query);
    }

    private void initialize(ADBJoinQuery query) {
        for(ADBJoinQueryPredicate predicate : query.getPredicates()) {
            this.initializeLeft(predicate);
            this.initializeRight(predicate);
        }
    }

    private void initializeLeft(ADBJoinQueryPredicate predicate) {
        if (minValuesLeft.containsKey(predicate.getLeftHandSideAttribute())) {
            this.initializeLeftDefault(predicate);
        } else if (LESS_OPERATORS.contains(predicate.getOperator())) {
            this.initializeForOperatorLessLeft(predicate);
        } else if (GREATER_OPERATORS.contains(predicate.getOperator())) {
            this.initializeForOperatorGreaterLeft(predicate);
        } else {
            this.initializeLeftDefault(predicate);
        }
    }

    private void initializeRight(ADBJoinQueryPredicate predicate) {
        if (minValuesRight.containsKey(predicate.getRightHandSideAttribute())) {
            this.initializeRightDefault(predicate);
        } else if (LESS_OPERATORS.contains(predicate.getOperator())) {
            this.initializeForOperatorLessRight(predicate);
        } else if (GREATER_OPERATORS.contains(predicate.getOperator())) {
            this.initializeForOperatorGreaterRight(predicate);
        } else {
            this.initializeRightDefault(predicate);
        }
    }

    private void initializeForOperatorLessLeft(ADBJoinQueryPredicate predicate) {
        val minLeft = this.joinTask.getLeftHeader().getMinValues().get(predicate.getLeftHandSideAttribute());
        this.minValuesLeft.put(predicate.getLeftHandSideAttribute(), minLeft);
        val maxLeft = this.joinTask.getRightHeader().getMaxValues().get(predicate.getRightHandSideAttribute());
        this.maxValuesLeft.put(predicate.getLeftHandSideAttribute(), maxLeft);
    }

    private void initializeForOperatorLessRight(ADBJoinQueryPredicate predicate) {
        val minRight = this.joinTask.getLeftHeader().getMinValues().get(predicate.getLeftHandSideAttribute());
        this.minValuesRight.put(predicate.getRightHandSideAttribute(), minRight);
        val maxRight = this.joinTask.getRightHeader().getMaxValues().get(predicate.getRightHandSideAttribute());
        this.maxValuesRight.put(predicate.getRightHandSideAttribute(), maxRight);
    }

    private void initializeForOperatorGreaterLeft(ADBJoinQueryPredicate predicate) {
        val minLeft = this.joinTask.getRightHeader().getMinValues().get(predicate.getRightHandSideAttribute());
        this.minValuesLeft.put(predicate.getLeftHandSideAttribute(), minLeft);
        val maxLeft = this.joinTask.getLeftHeader().getMaxValues().get(predicate.getLeftHandSideAttribute());
        this.maxValuesLeft.put(predicate.getLeftHandSideAttribute(), maxLeft);
    }

    private void initializeForOperatorGreaterRight(ADBJoinQueryPredicate predicate) {
        val minRight = this.joinTask.getRightHeader().getMinValues().get(predicate.getRightHandSideAttribute());
        this.minValuesRight.put(predicate.getRightHandSideAttribute(), minRight);
        val maxRight = this.joinTask.getLeftHeader().getMaxValues().get(predicate.getLeftHandSideAttribute());
        this.maxValuesRight.put(predicate.getRightHandSideAttribute(), maxRight);
    }

    private void initializeLeftDefault(ADBJoinQueryPredicate predicate) {
        val defaultMin = this.joinTask.getLeftHeader().getMinValues().get(predicate.getLeftHandSideAttribute());
        this.minValuesLeft.put(predicate.getLeftHandSideAttribute(), defaultMin);
        val defaultMax = this.joinTask.getLeftHeader().getMaxValues().get(predicate.getLeftHandSideAttribute());
        this.maxValuesLeft.put(predicate.getLeftHandSideAttribute(), defaultMax);
    }

    private void initializeRightDefault(ADBJoinQueryPredicate predicate) {
        val defaultMin = this.joinTask.getRightHeader().getMinValues().get(predicate.getRightHandSideAttribute());
        this.minValuesRight.put(predicate.getRightHandSideAttribute(), defaultMin);
        val defaultMax = this.joinTask.getRightHeader().getMaxValues().get(predicate.getRightHandSideAttribute());
        this.maxValuesRight.put(predicate.getRightHandSideAttribute(), defaultMax);
    }


    @Override
    public ADBEntityEntry getMinValueForLeft(String attributeName) {
        return this.minValuesLeft.get(attributeName);
    }

    @Override
    public ADBEntityEntry getMaxValueForLeft(String attributeName) {
        return this.maxValuesLeft.get(attributeName);
    }

    @Override
    public ADBEntityEntry getMinValueForRight(String attributeName) {
        return this.minValuesRight.get(attributeName);
    }

    @Override
    public ADBEntityEntry getMaxValueForRight(String attributeName) {
        return this.maxValuesRight.get(attributeName);
    }
}
