package de.hpi.julianweise.shard;

import de.hpi.julianweise.query.ADBSelectionQuery;
import org.junit.Test;

import static de.hpi.julianweise.query.ADBQuery.RelationalOperator.EQUALITY;
import static org.assertj.core.api.Assertions.assertThat;

public class ADBSelectionQueryTest {

    @Test
    public void addTermToQuery() {
        ADBSelectionQuery query = new ADBSelectionQuery();

        assertThat(query.getTerms().size()).isZero();

        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(EQUALITY)
                .value(1)
                .build();
        query.addTerm(term);

        assertThat(query.getTerms().size()).isEqualTo(1);
        assertThat(query.getTerms().get(0)).isEqualTo(term);
    }

    @Test
    public void queryTermStringRepresentation() {
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm.builder()
                                                                                        .fieldName("aInteger")
                                                                                        .operator(ADBSelectionQuery.RelationalOperator.EQUALITY)
                                                                                        .value(2)
                                                                                        .build();

        assertThat(term.toString()).contains("Term");
        assertThat(term.toString()).contains("2");
        assertThat(term.toString()).contains("aInteger");
        assertThat(term.toString()).contains("EQUALITY");
    }

    @Test
    public void queryStringRepresentation() {
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm.builder()
                                                                                        .fieldName("aInteger")
                                                                                        .operator(ADBSelectionQuery.RelationalOperator.EQUALITY)
                                                                                        .value(2)
                                                                                        .build();

        ADBSelectionQuery query = new ADBSelectionQuery();
        query.addTerm(term);

        assertThat(query.toString()).contains("Query");
        assertThat(query.toString()).contains("2");
        assertThat(query.toString()).contains("aInteger");
        assertThat(query.toString()).contains("EQUALITY");
    }

    @Test
    public void noArgsConstructorIsPresentForDeserialization() {
        ADBSelectionQuery.SelectionQueryTerm term = new ADBSelectionQuery.SelectionQueryTerm();

        assertThat(term.getFieldName()).isNull();
        assertThat(term.getValue()).isNull();
        assertThat(term.getOperator()).isNull();
    }

}