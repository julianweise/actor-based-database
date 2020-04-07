package de.hpi.julianweise.query;

import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ADBJoinQueryTest {

    @Test
    public void addTermsToQuery() {
        ADBJoinQueryTerm term = new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "test", "testTarget");
        ADBJoinQuery joinQuery = new ADBJoinQuery(Collections.singletonList(term));

        assertThat(joinQuery.terms.size()).isZero();
        joinQuery.addTerm(term);
        assertThat(joinQuery.terms.size()).isOne();
        assertThat(joinQuery.getTerms().get(0)).isEqualTo(term);
    }

    @Test
    public void queryHasInformativeStringRepresentation() {
        ADBJoinQueryTerm term = new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "test", "testTarget");
        ADBJoinQuery joinQuery = new ADBJoinQuery(Collections.singletonList(term));

        joinQuery.addTerm(term);
        assertThat(joinQuery.toString()).contains("test", "testTarget", "EQUALITY", "JoinQuery");
    }

}