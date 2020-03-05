package de.hpi.julianweise.domain;

import de.hpi.julianweise.query.ADBSelectionQuery;
import main.de.hpi.julianweise.csv.TestEntity;
import org.junit.Test;

import static de.hpi.julianweise.query.ADBSelectionQuery.RelationalOperator.UNSPECIFIED;
import static org.assertj.core.api.Assertions.assertThat;

public class ADBEntityTypeTest {

    // ##### Integer #####
    @Test
    public void matchesEqualityIntegerQueryTermSuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm(1, "aInteger", ADBSelectionQuery.RelationalOperator.EQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesInEqualityIntegerQueryTermSuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm(1, "aInteger", ADBSelectionQuery.RelationalOperator.INEQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesLessIntegerQueryTermSuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm(1, "aInteger", ADBSelectionQuery.RelationalOperator.LESS);

        TestEntity entity = new TestEntity(0, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesGreaterIntegerQueryTermSuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm(1, "aInteger", ADBSelectionQuery.RelationalOperator.GREATER);

        TestEntity entity = new TestEntity(0, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesLessOrEqualIntegerQueryTermSuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm(1, "aInteger", ADBSelectionQuery.RelationalOperator.LESS_OR_EQUAL);

        TestEntity entity = new TestEntity(0, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesGreaterOrEqualIntegerQueryTermSuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm(1, "aInteger", ADBSelectionQuery.RelationalOperator.GREATER_OR_EQUAL);

        TestEntity entity = new TestEntity(0, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    // ##### Float #####

    @Test
    public void matchesEqualityFloatQueryTermSuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm(1.01f, "cFloat", ADBSelectionQuery.RelationalOperator.EQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesInEqualityFloatQueryTermSuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm(1.01f, "cFloat", ADBSelectionQuery.RelationalOperator.INEQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesLessFloatQueryTermSuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm(1.01f, "cFloat", ADBSelectionQuery.RelationalOperator.LESS);

        TestEntity entity = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 2.01f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesGreaterFloatQueryTermSuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm(1.01f, "cFloat", ADBSelectionQuery.RelationalOperator.GREATER);

        TestEntity entity = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 2.01f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesLessOrEqualFloatQueryTermSuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm(1.01f, "cFloat",
                ADBSelectionQuery.RelationalOperator.LESS_OR_EQUAL);

        TestEntity entity = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(1, "Test", 1.02f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesGreaterOrEqualFloatQueryTermSuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm(1.01f, "cFloat",
                ADBSelectionQuery.RelationalOperator.GREATER_OR_EQUAL);

        TestEntity entity = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(1, "Test", 2.01f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    // ##### String #####

    @Test
    public void matchesEqualityStringQueryTermSuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm("Test", "bString", ADBSelectionQuery.RelationalOperator.EQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesInEqualityStringQueryTermSuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm("Test", "bString",
                ADBSelectionQuery.RelationalOperator.INEQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    // ##### Boolean #####

    @Test
    public void matchesEqualityBooleanQueryTermSuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm(true, "dBoolean", ADBSelectionQuery.RelationalOperator.EQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, false, 12.02, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesInEqualityBooleanQueryTermSuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm(true, "dBoolean",
                ADBSelectionQuery.RelationalOperator.INEQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, false, 12.02, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    // #### Defaults / Exceptions

    @Test
    public void defaultsToFalseForUnknownOperator() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm(1.01f, "cFloat", UNSPECIFIED);

        TestEntity entity = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 2.01f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    // #### Queries ####

    @Test
    public void matchesStringQuerySuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm("Test", "bString", ADBSelectionQuery.RelationalOperator.EQUALITY);
        ADBSelectionQuery query = new ADBSelectionQuery();
        query.addTerm(term);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(query)).isTrue();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(query)).isFalse();
    }

    @Test
    public void matchesQueryMultipleTermsSuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term1 = new ADBSelectionQuery.ABDQueryTerm("Test", "bString",
                ADBSelectionQuery.RelationalOperator.EQUALITY);
        ADBSelectionQuery.ABDQueryTerm term2 = new ADBSelectionQuery.ABDQueryTerm(1f, "cFloat",
                ADBSelectionQuery.RelationalOperator.INEQUALITY);
        ADBSelectionQuery query = new ADBSelectionQuery();
        query.addTerm(term1);
        query.addTerm(term2);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(query)).isTrue();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(query)).isFalse();
    }

    @Test
    public void matchesIntegerQuerySuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm(1, "aInteger",
                ADBSelectionQuery.RelationalOperator.EQUALITY);
        ADBSelectionQuery query = new ADBSelectionQuery();
        query.addTerm(term);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(query)).isTrue();

        TestEntity entity2 = new TestEntity(2, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(query)).isFalse();
    }

    @Test
    public void matchesFloatQuerySuccessfully() {
        ADBSelectionQuery.ABDQueryTerm term = new ADBSelectionQuery.ABDQueryTerm(1.01f, "cFloat",
                ADBSelectionQuery.RelationalOperator.EQUALITY);
        ADBSelectionQuery query = new ADBSelectionQuery();
        query.addTerm(term);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(query)).isTrue();

        TestEntity entity2 = new TestEntity(2, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(query)).isFalse();
    }
}