package de.hpi.julianweise.query.join;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.utility.list.ObjectArrayListCollector;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ADBJoinQuery implements ADBQuery {

    protected List<ADBJoinQueryPredicate> predicates = new ArrayList<>();
    protected boolean shouldBeMaterialized;

    public ADBJoinQuery(List<ADBJoinQueryPredicate> predicates) {
        this.predicates = predicates;
    }

    public void addPredicate(ADBJoinQueryPredicate predicate) {
        this.getPredicates().add(predicate);
    }

    public String[] getAllLeftHandSideFields() {
        return this.getPredicates().stream()
                   .map(ADBJoinQueryPredicate::getLeftHandSideAttribute)
                   .distinct()
                   .toArray(String[]::new);
    }

    public String[] getAllRightHandSideFields() {
        return this.getPredicates().stream()
                   .map(ADBJoinQueryPredicate::getRightHandSideAttribute)
                   .distinct()
                   .toArray(String[]::new);
    }


    @Override
    public String toString() {
        return "[JoinQuery] " + this.getPredicates().stream()
                                    .map(ADBJoinQueryPredicate::toString)
                                    .reduce((predicate, acc) -> acc + " & " + predicate).orElse("");
    }

    @JsonIgnore
    public ADBJoinQuery getReverse() {
        return new ADBJoinQuery(this.getPredicates().stream()
                                    .map(ADBJoinQueryPredicate::getReverse)
                                    .collect(new ObjectArrayListCollector<>()), this.shouldBeMaterialized);
    }
}
