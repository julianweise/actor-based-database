package de.hpi.julianweise.query;

import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ADBJoinQueryPredicateTest {

    @Test
    public void testToString() {
        ADBJoinQueryPredicate termUnderTest = new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "test",
                "testTarget");
        assertThat(termUnderTest.toString()).contains("testTarget", "test", "EQUALITY", "JoinTerm");
    }

}