package de.hpi.julianweise.slave.query.join.cost;

import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.query.join.ADBJoinQueryPredicate;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntry;
import de.hpi.julianweise.slave.query.join.cost.calculators.ADBJoinTermCostCalculator;
import de.hpi.julianweise.slave.query.join.cost.calculators.ADBJoinTermEqualityCostCalculator;
import de.hpi.julianweise.slave.query.join.cost.calculators.ADBJoinTermGreaterCostCalculator;
import de.hpi.julianweise.slave.query.join.cost.calculators.ADBJoinTermGreaterOrEqualCostCalculator;
import de.hpi.julianweise.slave.query.join.cost.calculators.ADBJoinTermInequalityCostCalculator;
import de.hpi.julianweise.slave.query.join.cost.calculators.ADBJoinTermLessCostCalculator;
import de.hpi.julianweise.slave.query.join.cost.calculators.ADBJoinTermLessOrEqualCostCalculator;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import org.agrona.collections.Object2ObjectHashMap;

import java.util.Map;

@AllArgsConstructor
public class ADBJoinPredicateCostModelFactory {

    private static final Map<ADBQueryTerm.RelationalOperator, ADBJoinTermCostCalculator> strategies =
            new Object2ObjectHashMap<ADBQueryTerm.RelationalOperator, ADBJoinTermCostCalculator>() {{
                put(ADBQueryTerm.RelationalOperator.EQUALITY, new ADBJoinTermEqualityCostCalculator());
                put(ADBQueryTerm.RelationalOperator.INEQUALITY, new ADBJoinTermInequalityCostCalculator());
                put(ADBQueryTerm.RelationalOperator.LESS, new ADBJoinTermLessCostCalculator());
                put(ADBQueryTerm.RelationalOperator.LESS_OR_EQUAL, new ADBJoinTermLessOrEqualCostCalculator());
                put(ADBQueryTerm.RelationalOperator.GREATER, new ADBJoinTermGreaterCostCalculator());
                put(ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL, new ADBJoinTermGreaterOrEqualCostCalculator());
            }};

    public static ADBJoinPredicateCostModel calc(ADBJoinQueryPredicate predicate,
                                                 ObjectList<ADBEntityEntry> left,
                                                 ObjectList<ADBEntityEntry> right) {
        ADBInterval[][] candidates = {};
        if (left.size() > 0 && right.size() > 0) {
            ADBComparator comparator = ADBComparator.getFor(left.get(0).getValueField(), right.get(0).getValueField());
            candidates = ADBJoinPredicateCostModelFactory.strategies.get(predicate.getOperator()).calc(left, right, comparator);
        }
        return ADBJoinPredicateCostModel.builder()
                                        .predicate(predicate)
                                        .joinCandidates(candidates)
                                        .sizeRight(right.size())
                                        .build();
    }

}
