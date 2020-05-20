package de.hpi.julianweise.query;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
@AllArgsConstructor
public class ADBSelectionQuery implements ADBQuery {

    @Getter
    protected final List<ADBSelectionQueryTerm> terms = new ArrayList<>();

    public void addTerm(ADBSelectionQueryTerm term) {
        this.terms.add(term);
    }

    @Override
    public String toString() {
        return "[SelectionQuery] " + this.terms.stream()
                                               .map(ADBSelectionQueryTerm::toString)
                                               .reduce((term, acc) -> acc + " & " + term).orElse("");
    }
}
