package de.hpi.julianweise.shard;

import de.hpi.julianweise.query.ADBQuery;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBQueryTest {

    @Test
    public void addTermToQuery() {
        ADBQuery query = new ADBQuery();

        assertThat(query.getTerms().size()).isZero();

        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm(1, "Test", ADBQuery.RelationalOperator.EQUALITY);

        query.addTerm(term);
        assertThat(query.getTerms().size()).isEqualTo(1);
        assertThat(query.getTerms().get(0)).isEqualTo(term);
    }

    @Test
    public void queryTermStringRepresentation() {
        ADBQuery.ABDQueryTerm term = ADBQuery.ABDQueryTerm.builder()
                                                          .fieldName("aInteger")
                                                          .operator(ADBQuery.RelationalOperator.EQUALITY)
                                                          .value(2)
                                                          .build();

        assertThat(term.toString()).contains("Term");
        assertThat(term.toString()).contains("2");
        assertThat(term.toString()).contains("aInteger");
        assertThat(term.toString()).contains("EQUALITY");
    }

    @Test
    public void queryStringRepresentation() {
        ADBQuery.ABDQueryTerm term = ADBQuery.ABDQueryTerm.builder()
                                                          .fieldName("aInteger")
                                                          .operator(ADBQuery.RelationalOperator.EQUALITY)
                                                          .value(2)
                                                          .build();

        ADBQuery query = new ADBQuery();
        query.addTerm(term);

        assertThat(query.toString()).contains("Query");
        assertThat(query.toString()).contains("2");
        assertThat(query.toString()).contains("aInteger");
        assertThat(query.toString()).contains("EQUALITY");
    }

    @Test
    public void noArgsConstructorIsPresentForDeserialization() {
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm();

        assertThat(term.getFieldName()).isNull();
        assertThat(term.getValue()).isNull();
        assertThat(term.getOperator()).isNull();
    }

}