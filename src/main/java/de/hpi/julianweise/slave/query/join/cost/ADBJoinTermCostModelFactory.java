package de.hpi.julianweise.slave.query.join.cost;

import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.query.join.ADBJoinQueryPredicate;
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
public class ADBJoinTermCostModelFactory {

    private static final Map<ADBQueryTerm.RelationalOperator, ADBJoinTermCostCalculator> strategies =
            new Object2ObjectHashMap<ADBQueryTerm.RelationalOperator, ADBJoinTermCostCalculator>() {{
                put(ADBQueryTerm.RelationalOperator.EQUALITY, new ADBJoinTermEqualityCostCalculator());
                put(ADBQueryTerm.RelationalOperator.INEQUALITY, new ADBJoinTermInequalityCostCalculator());
                put(ADBQueryTerm.RelationalOperator.LESS, new ADBJoinTermLessCostCalculator());
                put(ADBQueryTerm.RelationalOperator.LESS_OR_EQUAL, new ADBJoinTermLessOrEqualCostCalculator());
                put(ADBQueryTerm.RelationalOperator.GREATER, new ADBJoinTermGreaterCostCalculator());
                put(ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL, new ADBJoinTermGreaterOrEqualCostCalculator());
            }};

    public static ADBJoinTermCostModel calc(ADBJoinQueryPredicate predicate,
                                            int termId,
                                            ObjectList<ADBEntityEntry> left,
                                            ObjectList<ADBEntityEntry> right) {
        ADBInterval[] joinCandidates = ADBJoinTermCostModelFactory.strategies.get(predicate.getOperator()).calc(left, right);
        return ADBJoinTermCostModel.builder()
                                   .predicate(predicate)
                                   .joinCandidates(joinCandidates)
                                   .sizeLeft(left.size())
                                   .sizeRight(right.size())
                                   .termId(termId)
                                   .build();
    }

}
