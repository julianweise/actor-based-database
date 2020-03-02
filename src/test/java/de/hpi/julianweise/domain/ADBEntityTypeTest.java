package de.hpi.julianweise.domain;

import de.hpi.julianweise.query.ADBQuery;
import main.de.hpi.julianweise.csv.TestEntity;
import org.junit.Test;

import static de.hpi.julianweise.query.ADBQuery.RelationalOperator.UNSPECIFIED;
import static org.assertj.core.api.Assertions.assertThat;

public class ADBEntityTypeTest {

    @Test
    public void hasFieldDetectsFieldSuccessfully() {
        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.hasField("aInteger")).isTrue();
    }

    @Test
    public void hasFieldDetectsNotPresentFieldSuccessfully() {
        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.hasField("aNotPresentField")).isFalse();
    }

    // ##### Integer #####

    @Test
    public void matchesEqualityIntegerQueryTermSuccessfully() {
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm(1, "aInteger", ADBQuery.RelationalOperator.EQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesInEqualityIntegerQueryTermSuccessfully() {
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm(1, "aInteger", ADBQuery.RelationalOperator.INEQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesLessIntegerQueryTermSuccessfully() {
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm(1, "aInteger", ADBQuery.RelationalOperator.LESS);

        TestEntity entity = new TestEntity(0, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesGreaterIntegerQueryTermSuccessfully() {
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm(1, "aInteger", ADBQuery.RelationalOperator.GREATER);

        TestEntity entity = new TestEntity(0, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesLessOrEqualIntegerQueryTermSuccessfully() {
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm(1, "aInteger", ADBQuery.RelationalOperator.LESS_OR_EQUAL);

        TestEntity entity = new TestEntity(0, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesGreaterOrEqualIntegerQueryTermSuccessfully() {
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm(1, "aInteger", ADBQuery.RelationalOperator.GREATER_OR_EQUAL);

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
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm(1.01f, "cFloat", ADBQuery.RelationalOperator.EQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesInEqualityFloatQueryTermSuccessfully() {
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm(1.01f, "cFloat", ADBQuery.RelationalOperator.INEQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesLessFloatQueryTermSuccessfully() {
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm(1.01f, "cFloat", ADBQuery.RelationalOperator.LESS);

        TestEntity entity = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 2.01f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesGreaterFloatQueryTermSuccessfully() {
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm(1.01f, "cFloat", ADBQuery.RelationalOperator.GREATER);

        TestEntity entity = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 2.01f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesLessOrEqualFloatQueryTermSuccessfully() {
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm(1.01f, "cFloat",
                ADBQuery.RelationalOperator.LESS_OR_EQUAL);

        TestEntity entity = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(1, "Test", 1.02f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesGreaterOrEqualFloatQueryTermSuccessfully() {
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm(1.01f, "cFloat",
                ADBQuery.RelationalOperator.GREATER_OR_EQUAL);

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
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm("Test", "bString", ADBQuery.RelationalOperator.EQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesInEqualityStringQueryTermSuccessfully() {
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm("Test", "bString",
                ADBQuery.RelationalOperator.INEQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    // ##### Boolean #####

    @Test
    public void matchesEqualityBooleanQueryTermSuccessfully() {
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm(true, "dBoolean", ADBQuery.RelationalOperator.EQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, false, 12.02, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesInEqualityBooleanQueryTermSuccessfully() {
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm(true, "dBoolean",
                ADBQuery.RelationalOperator.INEQUALITY);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, false, 12.02, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    // #### Defaults / Exceptions

    @Test
    public void defaultsToFalseForUnknownOperator() {
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm(1.01f, "cFloat", UNSPECIFIED);

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
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm("Test", "bString", ADBQuery.RelationalOperator.EQUALITY);
        ADBQuery query = new ADBQuery();
        query.addTerm(term);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(query)).isTrue();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(query)).isFalse();
    }

    @Test
    public void matchesQueryMultipleTermsSuccessfully() {
        ADBQuery.ABDQueryTerm term1 = new ADBQuery.ABDQueryTerm("Test", "bString",
                ADBQuery.RelationalOperator.EQUALITY);
        ADBQuery.ABDQueryTerm term2 = new ADBQuery.ABDQueryTerm(1f, "cFloat",
                ADBQuery.RelationalOperator.INEQUALITY);
        ADBQuery query = new ADBQuery();
        query.addTerm(term1);
        query.addTerm(term2);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(query)).isTrue();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(query)).isFalse();
    }

    @Test
    public void matchesIntegerQuerySuccessfully() {
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm(1, "aInteger",
                ADBQuery.RelationalOperator.EQUALITY);
        ADBQuery query = new ADBQuery();
        query.addTerm(term);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(query)).isTrue();

        TestEntity entity2 = new TestEntity(2, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(query)).isFalse();
    }

    @Test
    public void matchesFloatQuerySuccessfully() {
        ADBQuery.ABDQueryTerm term = new ADBQuery.ABDQueryTerm(1.01f, "cFloat",
                ADBQuery.RelationalOperator.EQUALITY);
        ADBQuery query = new ADBQuery();
        query.addTerm(term);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(query)).isTrue();

        TestEntity entity2 = new TestEntity(2, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(query)).isFalse();
    }
}