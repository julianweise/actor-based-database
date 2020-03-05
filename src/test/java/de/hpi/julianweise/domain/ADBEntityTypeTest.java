package de.hpi.julianweise.domain;

import de.hpi.julianweise.query.ADBSelectionQuery;
import main.de.hpi.julianweise.csv.TestEntity;
import org.junit.Test;

import static de.hpi.julianweise.query.ADBQuery.RelationalOperator.EQUALITY;
import static de.hpi.julianweise.query.ADBQuery.RelationalOperator.GREATER;
import static de.hpi.julianweise.query.ADBQuery.RelationalOperator.GREATER_OR_EQUAL;
import static de.hpi.julianweise.query.ADBQuery.RelationalOperator.INEQUALITY;
import static de.hpi.julianweise.query.ADBQuery.RelationalOperator.LESS;
import static de.hpi.julianweise.query.ADBQuery.RelationalOperator.LESS_OR_EQUAL;
import static de.hpi.julianweise.query.ADBQuery.RelationalOperator.UNSPECIFIED;
import static org.assertj.core.api.Assertions.assertThat;

public class ADBEntityTypeTest {

    // ##### Integer #####
    @Test
    public void matchesEqualityIntegerQueryTermSuccessfully() {
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(EQUALITY)
                .value(1)
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesInEqualityIntegerQueryTermSuccessfully() {
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(INEQUALITY)
                .value(1)
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesLessIntegerQueryTermSuccessfully() {
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(LESS)
                .value(1)
                .build();

        TestEntity entity = new TestEntity(0, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesGreaterIntegerQueryTermSuccessfully() {
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(GREATER)
                .value(1)
                .build();

        TestEntity entity = new TestEntity(0, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesLessOrEqualIntegerQueryTermSuccessfully() {
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(LESS_OR_EQUAL)
                .value(1)
                .build();

        TestEntity entity = new TestEntity(0, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesGreaterOrEqualIntegerQueryTermSuccessfully() {
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(GREATER_OR_EQUAL)
                .value(1)
                .build();

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
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("cFloat")
                .operator(EQUALITY)
                .value(1.01f)
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesInEqualityFloatQueryTermSuccessfully() {
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("cFloat")
                .operator(INEQUALITY)
                .value(1.01f)
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesLessFloatQueryTermSuccessfully() {
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("cFloat")
                .operator(LESS)
                .value(1.01f)
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 2.01f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesGreaterFloatQueryTermSuccessfully() {
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("cFloat")
                .operator(GREATER)
                .value(1.01f)
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 2.01f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesLessOrEqualFloatQueryTermSuccessfully() {
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("cFloat")
                .operator(LESS_OR_EQUAL)
                .value(1.01f)
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(1, "Test", 1.02f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesGreaterOrEqualFloatQueryTermSuccessfully() {
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("cFloat")
                .operator(GREATER_OR_EQUAL)
                .value(1.01f)
                .build();

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
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("bString")
                .operator(EQUALITY)
                .value("Test")
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesInEqualityStringQueryTermSuccessfully() {
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("bString")
                .operator(INEQUALITY)
                .value("Test")
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    // ##### Boolean #####

    @Test
    public void matchesEqualityBooleanQueryTermSuccessfully() {
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("dBoolean")
                .operator(EQUALITY)
                .value(true)
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, false, 12.02, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesInEqualityBooleanQueryTermSuccessfully() {
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("dBoolean")
                .operator(INEQUALITY)
                .value(true)
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, false, 12.02, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    // #### Defaults / Exceptions

    @Test
    public void defaultsToFalseForUnknownOperator() {
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("cFloat")
                .operator(UNSPECIFIED)
                .value(1.01f)
                .build();


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
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("bString")
                .operator(EQUALITY)
                .value("Test")
                .build();

        ADBSelectionQuery query = new ADBSelectionQuery();
        query.addTerm(term);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(query)).isTrue();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(query)).isFalse();
    }

    @Test
    public void matchesQueryMultipleTermsSuccessfully() {
        ADBSelectionQuery.SelectionQueryTerm term1 = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("bString")
                .operator(EQUALITY)
                .value("Test")
                .build();

        ADBSelectionQuery.SelectionQueryTerm term2 = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("cFloat")
                .operator(INEQUALITY)
                .value(1f)
                .build();

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
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(EQUALITY)
                .value(1)
                .build();

        ADBSelectionQuery query = new ADBSelectionQuery();
        query.addTerm(term);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(query)).isTrue();

        TestEntity entity2 = new TestEntity(2, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(query)).isFalse();
    }

    @Test
    public void matchesFloatQuerySuccessfully() {
        ADBSelectionQuery.SelectionQueryTerm term = ADBSelectionQuery.SelectionQueryTerm
                .builder()
                .fieldName("cFloat")
                .operator(EQUALITY)
                .value(1.01f)
                .build();

        ADBSelectionQuery query = new ADBSelectionQuery();
        query.addTerm(term);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity.matches(query)).isTrue();

        TestEntity entity2 = new TestEntity(2, "TestNotEqual", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(query)).isFalse();
    }
}