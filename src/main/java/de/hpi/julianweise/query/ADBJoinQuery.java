package de.hpi.julianweise.query;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Setter
@Getter
@AllArgsConstructor
public class ADBJoinQuery implements ADBQuery {

    @Getter
    protected final List<ADBJoinQueryTerm> terms = new ArrayList<>();

    public void addTerm(ADBJoinQueryTerm term) {
        this.terms.add(term);
    }

    public Set<String> getAllFields() {
        return this.getTerms().stream()
            .map(term -> {
                List<String> fields = new ArrayList<>(2);
                fields.add(term.getLeftHandSideAttribute());
                fields.add(term.getRightHandSideAttribute());
                return fields;
            })
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());
    }

    @Override
    public String toString() {
        return "[JoinQuery] " + this.terms.stream()
                                          .map(ADBJoinQueryTerm::toString)
                                          .reduce((term, acc) -> acc + " & " + term).orElse("");
    }
}
