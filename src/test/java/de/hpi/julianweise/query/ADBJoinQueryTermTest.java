package de.hpi.julianweise.query;

import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ADBJoinQueryTermTest {

    @Test
    public void testToString() {
        ADBJoinQueryTerm termUnderTest = new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "test",
                "testTarget");
        assertThat(termUnderTest.toString()).contains("testTarget", "test", "EQUALITY", "JoinTerm");
    }

}