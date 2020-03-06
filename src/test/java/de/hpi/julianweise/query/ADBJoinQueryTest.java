package de.hpi.julianweise.query;

import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ADBJoinQueryTest {

    @Test
    public void addTermsToQuery() {
        ADBJoinQuery joinQuery = new ADBJoinQuery();
        ADBJoinQueryTerm term = new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "test", "testTarget");

        assertThat(joinQuery.terms.size()).isZero();
        joinQuery.addTerm(term);
        assertThat(joinQuery.terms.size()).isOne();
        assertThat(joinQuery.getTerms().get(0)).isEqualTo(term);
    }

    @Test
    public void queryHasInformativeStringRepresentation() {
        ADBJoinQuery joinQuery = new ADBJoinQuery();
        ADBJoinQueryTerm term = new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "test", "testTarget");

        joinQuery.addTerm(term);
        assertThat(joinQuery.toString()).contains("test", "testTarget", "EQUALITY", "JoinQuery");
    }

}