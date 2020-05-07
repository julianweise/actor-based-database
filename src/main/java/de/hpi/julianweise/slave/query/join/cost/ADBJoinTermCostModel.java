package de.hpi.julianweise.slave.query.join.cost;

import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.util.Arrays;

@AllArgsConstructor
@Builder
public class ADBJoinTermCostModel {
    private final ADBJoinQueryTerm term;
    private final ADBInterval[] joinCandidates;

    public int getCost() {
        return Arrays.stream(this.joinCandidates).mapToInt(ADBInterval::size).sum();
    }
}
