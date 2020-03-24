package de.hpi.julianweise.query;

import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ADBSelectionQueryTermTest {

    @Test
    public void testToString() {
        ADBSelectionQueryTerm termUnderTest = new ADBSelectionQueryTerm(1, "test", ADBQueryTerm.RelationalOperator.EQUALITY);
        assertThat(termUnderTest.toString()).contains("1", "test", "EQUALITY", "SelectionTerm");
    }

}