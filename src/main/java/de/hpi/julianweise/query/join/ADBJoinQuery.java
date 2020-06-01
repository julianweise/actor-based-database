package de.hpi.julianweise.query.join;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.hpi.julianweise.query.ADBQuery;
import de.hpi.julianweise.utility.list.ObjectArrayListCollector;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

    public Set<String> getAllFields() {
        return this.getPredicates().stream()
                   .map(predicate -> {
                ObjectList<String> fields = new ObjectArrayList<>(2);
                fields.add(predicate.getLeftHandSideAttribute());
                fields.add(predicate.getRightHandSideAttribute());
                return fields;
            })
                   .flatMap(Collection::stream)
                   .collect(Collectors.toSet());
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
