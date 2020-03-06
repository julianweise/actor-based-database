package de.hpi.julianweise.query;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
@AllArgsConstructor
public class ADBJoinQuery implements ADBQuery {

    @Getter
    protected final List<ADBJoinQueryTerm> terms = new ArrayList<>();

    public void addTerm(ADBJoinQueryTerm term) {
        this.terms.add(term);
    }

    @Override
    public String toString() {
        return "[JoinQuery] " + this.terms.stream()
                                          .map(ADBJoinQueryTerm::toString)
                                          .reduce((term, acc) -> acc + " & " + term).orElse("");
    }
}
