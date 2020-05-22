package de.hpi.julianweise.query;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
import java.util.stream.Collector;
import java.util.stream.Collectors;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ADBJoinQuery implements ADBQuery {

    protected List<ADBJoinQueryTerm> terms = new ArrayList<>();
    protected boolean isMaterialized;

    public ADBJoinQuery(List<ADBJoinQueryTerm> terms) {
        this.terms = terms;
    }

    public void addTerm(ADBJoinQueryTerm term) {
        this.getTerms().add(term);
    }

    public Set<String> getAllFields() {
        return this.getTerms().stream()
            .map(term -> {
                ObjectList<String> fields = new ObjectArrayList<>(2);
                fields.add(term.getLeftHandSideAttribute());
                fields.add(term.getRightHandSideAttribute());
                return fields;
            })
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
    }

    @Override
    public String toString() {
        return "[JoinQuery] " + this.getTerms().stream()
                                          .map(ADBJoinQueryTerm::toString)
                                          .reduce((term, acc) -> acc + " & " + term).orElse("");
    }

    @JsonIgnore
    public ADBJoinQuery getReverse() {
        return new ADBJoinQuery(this.getTerms().stream().map(ADBJoinQueryTerm::getReverse)
                                    .collect(Collector.of(
                                            ObjectArrayList::new,
                                            ObjectArrayList::add,
                                            (collection1, collection2) -> {
                                                collection2.addAll(collection1);
                                                return collection2;
                                            }
                                    )), this.isMaterialized);
    }
}
