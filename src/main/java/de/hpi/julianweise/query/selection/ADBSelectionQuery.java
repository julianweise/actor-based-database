package de.hpi.julianweise.query.selection;

import de.hpi.julianweise.query.ADBQuery;
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
    protected final List<ADBSelectionQueryPredicate> predicates = new ArrayList<>();

    public void addPredicate(ADBSelectionQueryPredicate predicate) {
        this.predicates.add(predicate);
    }

    @Override
    public String toString() {
        return "[SelectionQuery] " + this.predicates.stream()
                                                    .map(ADBSelectionQueryPredicate::toString)
                                                    .reduce((predicate, acc) -> acc + " & " + predicate).orElse("");
    }
}
