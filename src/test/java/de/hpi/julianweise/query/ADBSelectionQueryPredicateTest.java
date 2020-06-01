package de.hpi.julianweise.query;

import de.hpi.julianweise.query.selection.ADBSelectionQueryPredicate;
import de.hpi.julianweise.query.selection.constant.ADBPredicateIntConstant;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ADBSelectionQueryPredicateTest {

    @Test
    public void testToString() {
        ADBSelectionQueryPredicate termUnderTest = new ADBSelectionQueryPredicate(new ADBPredicateIntConstant(1),
                "test",
                ADBQueryTerm.RelationalOperator.EQUALITY);
        assertThat(termUnderTest.toString()).contains("1", "test", "EQUALITY", "SelectionPredicate");
    }

}