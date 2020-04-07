package de.hpi.julianweise.query;

import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ADBJoinQueryTest {

    @Test
    public void initializeUsingConstructor() {
        ADBJoinQueryTerm term = new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "test", "testTarget");
        ADBJoinQuery joinQuery = new ADBJoinQuery(Collections.singletonList(term));

        assertThat(joinQuery.getTerms().size()).isOne();
        assertThat(joinQuery.getTerms().get(0)).isEqualTo(term);
    }

    @Test
    public void addTermsToQuery() {
        ADBJoinQueryTerm term = new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "test", "testTarget");
        ADBJoinQuery joinQuery = new ADBJoinQuery();

        assertThat(joinQuery.getTerms().size()).isZero();
        joinQuery.addTerm(term);
        assertThat(joinQuery.getTerms().size()).isOne();
        assertThat(joinQuery.getTerms().get(0)).isEqualTo(term);
    }

    @Test
    public void queryHasInformativeStringRepresentation() {
        ADBJoinQueryTerm term = new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "test", "testTarget");
        ADBJoinQuery joinQuery = new ADBJoinQuery(Collections.singletonList(term));

        assertThat(joinQuery.toString()).contains("test", "testTarget", "EQUALITY", "JoinQuery");
    }

    @Test
    public void queryReversesAllTerms() {
        ADBJoinQueryTerm term = new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "test", "testTarget");
        ADBJoinQueryTerm term2 = new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "test", "testTarget");
        ADBJoinQuery joinQuery = new ADBJoinQuery();

        joinQuery.addTerm(term);
        joinQuery.addTerm(term2);

        assertThat(joinQuery.terms.size()).isEqualTo(2);
        assertThat(joinQuery.getTerms().get(0)).isEqualTo(term);
        assertThat(joinQuery.getTerms().get(1)).isEqualTo(term2);

        ADBJoinQuery reversedJoinQuery = joinQuery.getReverse();

        assertThat(reversedJoinQuery.terms.size()).isEqualTo(2);
        assertThat(reversedJoinQuery.getTerms().get(0)).isNotEqualTo(term);
        assertThat(reversedJoinQuery.getTerms().get(1)).isNotEqualTo(term2);

        assertThat(reversedJoinQuery.getTerms().get(0).getLeftHandSideAttribute()).isEqualTo(joinQuery.getTerms().get(0).getRightHandSideAttribute());
        assertThat(reversedJoinQuery.getTerms().get(0).getRightHandSideAttribute()).isEqualTo(joinQuery.getTerms().get(0).getLeftHandSideAttribute());
        assertThat(reversedJoinQuery.getTerms().get(0).getOperator()).isEqualTo(ADBQueryTerm.RelationalOperator.EQUALITY);

        assertThat(reversedJoinQuery.getTerms().get(1).getLeftHandSideAttribute()).isEqualTo(joinQuery.getTerms().get(1).getRightHandSideAttribute());
        assertThat(reversedJoinQuery.getTerms().get(1).getRightHandSideAttribute()).isEqualTo(joinQuery.getTerms().get(1).getLeftHandSideAttribute());
        assertThat(reversedJoinQuery.getTerms().get(1).getOperator()).isEqualTo(ADBQueryTerm.RelationalOperator.GREATER);
    }

    @Test
    public void getAllFieldsUniquely() {
        ADBJoinQueryTerm term = new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "test", "testTarget1");
        ADBJoinQueryTerm term2 = new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "test", "testTarget2");
        ADBJoinQueryTerm term3 = new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "test2", "testTarget1");
        ADBJoinQuery joinQuery = new ADBJoinQuery();

        joinQuery.addTerm(term);
        joinQuery.addTerm(term2);
        joinQuery.addTerm(term3);

        Set<String> uniqueFields = joinQuery.getAllFields();

        assertThat(uniqueFields.size()).isEqualTo(4);
        assertThat(uniqueFields.contains("test")).isTrue();
        assertThat(uniqueFields.contains("testTarget1")).isTrue();
        assertThat(uniqueFields.contains("test2")).isTrue();
        assertThat(uniqueFields.contains("testTarget2")).isTrue();
    }

}