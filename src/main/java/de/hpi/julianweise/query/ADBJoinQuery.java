package de.hpi.julianweise.query;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

public class ADBJoinQuery extends ADBQuery {

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class JoinQueryTerm extends QueryTerm {
        @Override
        public String toString() {
            return "[JoinTerm] " + this.fieldName + " " + this.operator + " " + this.targetFieldName;
        }

        private String targetFieldName;
    }

    @Getter
    private final List<JoinQueryTerm> terms = new ArrayList<>();

    public void addTerm(JoinQueryTerm term) {
        this.terms.add(term);
    }

    @Override
    public String toString() {
        return "[JoinQuery] " + this.terms.stream()
                                      .map(JoinQueryTerm::toString)
                                      .reduce((term, acc) -> acc + " & " + term).orElse("");
    }
}
