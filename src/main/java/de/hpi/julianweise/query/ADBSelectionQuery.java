package de.hpi.julianweise.query;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@AllArgsConstructor
public class ADBSelectionQuery implements ADBQuery {

    @Getter
    protected final ObjectList<ADBSelectionQueryTerm> terms = new ObjectArrayList<>();

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
