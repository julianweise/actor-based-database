package de.hpi.julianweise.domain;

import de.hpi.julianweise.query.ADBSelectionQuery;
import main.de.hpi.julianweise.csv.TestEntity;
import org.junit.Test;

import static de.hpi.julianweise.query.ADBQuery.RelationalOperator.UNSPECIFIED;
import static org.assertj.core.api.Assertions.assertThat;

public class ADBEntityTypeTest {

    // ##### Integer #####
    @Test
    public void matchesEqualityIntegerQueryTermSuccessfully() {
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm(1, "aInteger", ADBSelectionQuery.RelationalOperator.EQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesInEqualityIntegerQueryTermSuccessfully() {
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm(1, "aInteger", ADBSelectionQuery.RelationalOperator.INEQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesLessIntegerQueryTermSuccessfully() {
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm(1, "aInteger", ADBSelectionQuery.RelationalOperator.LESS);

        TestEntity entity = new TestEntity(0, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesGreaterIntegerQueryTermSuccessfully() {
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm(1, "aInteger", ADBSelectionQuery.RelationalOperator.GREATER);

        TestEntity entity = new TestEntity(0, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesLessOrEqualIntegerQueryTermSuccessfully() {
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm(1, "aInteger", ADBSelectionQuery.RelationalOperator.LESS_OR_EQUAL);

        TestEntity entity = new TestEntity(0, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesGreaterOrEqualIntegerQueryTermSuccessfully() {
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm(1, "aInteger", ADBSelectionQuery.RelationalOperator.GREATER_OR_EQUAL);

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
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm(1.01f, "cFloat", ADBSelectionQuery.RelationalOperator.EQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesInEqualityFloatQueryTermSuccessfully() {
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm(1.01f, "cFloat", ADBSelectionQuery.RelationalOperator.INEQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesLessFloatQueryTermSuccessfully() {
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm(1.01f, "cFloat", ADBSelectionQuery.RelationalOperator.LESS);

        TestEntity entity = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 2.01f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesGreaterFloatQueryTermSuccessfully() {
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm(1.01f, "cFloat", ADBSelectionQuery.RelationalOperator.GREATER);

        TestEntity entity = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 2.01f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesLessOrEqualFloatQueryTermSuccessfully() {
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm(1.01f, "cFloat",
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
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm(1.01f, "cFloat",
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
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm("Test", "bString", ADBSelectionQuery.RelationalOperator.EQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesInEqualityStringQueryTermSuccessfully() {
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm("Test", "bString",
                ADBSelectionQuery.RelationalOperator.INEQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    // ##### Boolean #####

    @Test
    public void matchesEqualityBooleanQueryTermSuccessfully() {
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm(true, "dBoolean", ADBSelectionQuery.RelationalOperator.EQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, false, 12.02, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesInEqualityBooleanQueryTermSuccessfully() {
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm(true, "dBoolean",
                ADBSelectionQuery.RelationalOperator.INEQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, false, 12.02, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    // #### Defaults / Exceptions

    @Test
    public void defaultsToFalseForUnknownOperator() {
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm(1.01f, "cFloat", UNSPECIFIED);

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
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm("Test", "bString", ADBSelectionQuery.RelationalOperator.EQUALITY);
        ADBSelectionQuery query = new ADBSelectionQuery();
        query.addTerm(term);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(query)).isTrue();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(query)).isFalse();
    }

    @Test
    public void matchesQueryMultipleTermsSuccessfully() {
        ADBSelectionQuery.QueryTerm term1 = new ADBSelectionQuery.QueryTerm("Test", "bString",
                ADBSelectionQuery.RelationalOperator.EQUALITY);
        ADBSelectionQuery.QueryTerm term2 = new ADBSelectionQuery.QueryTerm(1f, "cFloat",
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
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm(1, "aInteger",
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
        ADBSelectionQuery.QueryTerm term = new ADBSelectionQuery.QueryTerm(1.01f, "cFloat",
                ADBSelectionQuery.RelationalOperator.EQUALITY);
        ADBSelectionQuery query = new ADBSelectionQuery();
        query.addTerm(term);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(query)).isTrue();

        TestEntity entity2 = new TestEntity(2, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(query)).isFalse();
    }
}