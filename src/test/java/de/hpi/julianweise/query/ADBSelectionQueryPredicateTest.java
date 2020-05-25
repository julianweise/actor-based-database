package de.hpi.julianweise.query;

import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ADBSelectionQueryPredicateTest {

    @Test
    public void testToString() {
        ADBSelectionQueryPredicate termUnderTest = new ADBSelectionQueryPredicate(1, "test", ADBQueryTerm.RelationalOperator.EQUALITY);
        assertThat(termUnderTest.toString()).contains("1", "test", "EQUALITY", "SelectionPredicate");
    }

}