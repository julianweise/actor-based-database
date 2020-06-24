package de.hpi.julianweise.query;

import de.hpi.julianweise.query.join.ADBJoinQuery;
import de.hpi.julianweise.query.join.ADBJoinQueryPredicate;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ADBJoinQueryTest {

    @Test
    public void initializeUsingConstructor() {
        ADBJoinQueryPredicate predicate = new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "test", "testTarget");
        ObjectList<ADBJoinQueryPredicate> termList = new ObjectArrayList<>();
        termList.add(predicate);
        ADBJoinQuery joinQuery = new ADBJoinQuery(termList);

        assertThat(joinQuery.getPredicates().size()).isOne();
        assertThat(joinQuery.getPredicates().get(0)).isEqualTo(predicate);
    }

    @Test
    public void addTermsToQuery() {
        ADBJoinQueryPredicate predicate = new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "test", "testTarget");
        ADBJoinQuery joinQuery = new ADBJoinQuery();

        assertThat(joinQuery.getPredicates().size()).isZero();
        joinQuery.addPredicate(predicate);
        assertThat(joinQuery.getPredicates().size()).isOne();
        assertThat(joinQuery.getPredicates().get(0)).isEqualTo(predicate);
    }

    @Test
    public void queryHasInformativeStringRepresentation() {
        ADBJoinQueryPredicate predicate = new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "test", "testTarget");
        ObjectList<ADBJoinQueryPredicate> termList = new ObjectArrayList<>();
        termList.add(predicate);
        ADBJoinQuery joinQuery = new ADBJoinQuery(termList);

        assertThat(joinQuery.toString()).contains("test", "testTarget", "EQUALITY", "JoinQuery");
    }

    @Test
    public void queryReversesAllTerms() {
        ADBJoinQueryPredicate predicate = new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "test", "testTarget");
        ADBJoinQueryPredicate term2 = new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.LESS, "test", "testTarget");
        ADBJoinQuery joinQuery = new ADBJoinQuery();

        joinQuery.addPredicate(predicate);
        joinQuery.addPredicate(term2);

        assertThat(joinQuery.getPredicates().size()).isEqualTo(2);
        assertThat(joinQuery.getPredicates().get(0)).isEqualTo(predicate);
        assertThat(joinQuery.getPredicates().get(1)).isEqualTo(term2);

        ADBJoinQuery reversedJoinQuery = joinQuery.getReverse();

        assertThat(reversedJoinQuery.getPredicates().size()).isEqualTo(2);
        assertThat(reversedJoinQuery.getPredicates().get(0)).isNotEqualTo(predicate);
        assertThat(reversedJoinQuery.getPredicates().get(1)).isNotEqualTo(term2);

        assertThat(reversedJoinQuery.getPredicates().get(0).getLeftHandSideAttribute()).isEqualTo(joinQuery.getPredicates().get(0).getRightHandSideAttribute());
        assertThat(reversedJoinQuery.getPredicates().get(0).getRightHandSideAttribute()).isEqualTo(joinQuery.getPredicates().get(0).getLeftHandSideAttribute());
        assertThat(reversedJoinQuery.getPredicates().get(0).getOperator()).isEqualTo(ADBQueryTerm.RelationalOperator.EQUALITY);

        assertThat(reversedJoinQuery.getPredicates().get(1).getLeftHandSideAttribute()).isEqualTo(joinQuery.getPredicates().get(1).getRightHandSideAttribute());
        assertThat(reversedJoinQuery.getPredicates().get(1).getRightHandSideAttribute()).isEqualTo(joinQuery.getPredicates().get(1).getLeftHandSideAttribute());
        assertThat(reversedJoinQuery.getPredicates().get(1).getOperator()).isEqualTo(ADBQueryTerm.RelationalOperator.GREATER);
    }

    @Test
    public void getAllFieldsUniquely() {
        ADBJoinQueryPredicate predicate = new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "test", "testTarget1");
        ADBJoinQueryPredicate term2 = new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.LESS, "test", "testTarget2");
        ADBJoinQueryPredicate term3 = new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.LESS, "test2", "testTarget1");
        ADBJoinQuery joinQuery = new ADBJoinQuery();

        joinQuery.addPredicate(predicate);
        joinQuery.addPredicate(term2);
        joinQuery.addPredicate(term3);

        String[] uniqueLeftFields = joinQuery.getAllLeftHandSideFields();

        assertThat(uniqueLeftFields.length).isEqualTo(2);
        assertThat(uniqueLeftFields[0]).isEqualTo("test");
        assertThat(uniqueLeftFields[1]).isEqualTo("test2");

        String[] uniqueRightFields = joinQuery.getAllRightHandSideFields();

        assertThat(uniqueRightFields.length).isEqualTo(2);
        assertThat(uniqueRightFields[0]).isEqualTo("testTarget1");
        assertThat(uniqueRightFields[1]).isEqualTo("testTarget2");
    }

}