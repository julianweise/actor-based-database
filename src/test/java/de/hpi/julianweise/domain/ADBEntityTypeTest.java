package de.hpi.julianweise.domain;

import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.query.ADBSelectionQuery;
import de.hpi.julianweise.query.ADBSelectionQueryTerm;
import org.junit.Test;

import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.EQUALITY;
import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.GREATER;
import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL;
import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.INEQUALITY;
import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.LESS;
import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.LESS_OR_EQUAL;
import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.UNSPECIFIED;
import static org.assertj.core.api.Assertions.assertThat;

public class ADBEntityTypeTest {

    // ##### Integer #####
    @Test
    public void matchesEqualityIntegerQueryTermSuccessfully() {
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
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
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
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
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(LESS)
                .value(1)
                .build();

        TestEntity entity = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(0, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesGreaterIntegerQueryTermSuccessfully() {
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(GREATER)
                .value(1)
                .build();

        TestEntity entity = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(0, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesLessOrEqualIntegerQueryTermSuccessfully() {
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(LESS_OR_EQUAL)
                .value(1)
                .build();

        TestEntity entity = new TestEntity(0, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesGreaterOrEqualIntegerQueryTermSuccessfully() {
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("aInteger")
                .operator(GREATER_OR_EQUAL)
                .value(1)
                .build();

        TestEntity entity = new TestEntity(0, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    // ##### Float #####

    @Test
    public void matchesEqualityFloatQueryTermSuccessfully() {
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
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
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
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
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("cFloat")
                .operator(LESS)
                .value(1.01f)
                .build();

        TestEntity entity = new TestEntity(1, "Test", 2.00f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 2.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 0.01f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    @Test
    public void matchesGreaterFloatQueryTermSuccessfully() {
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("cFloat")
                .operator(GREATER)
                .value(1.01f)
                .build();

        TestEntity entity = new TestEntity(1, "Test", 2.00f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 1.00f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesLessOrEqualFloatQueryTermSuccessfully() {
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("cFloat")
                .operator(LESS_OR_EQUAL)
                .value(1.01f)
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isTrue();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(1, "Test", 1.02f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isFalse();
    }

    @Test
    public void matchesGreaterOrEqualFloatQueryTermSuccessfully() {
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
                .builder()
                .fieldName("cFloat")
                .operator(GREATER_OR_EQUAL)
                .value(1.01f)
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.00f, true, 12.94232, 'w');
        assertThat(entity.matches(term)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02, 'w');
        assertThat(entity1.matches(term)).isTrue();

        TestEntity entity2 = new TestEntity(1, "Test", 2.01f, true, 12.02, 'w');
        assertThat(entity2.matches(term)).isTrue();
    }

    // ##### String #####

    @Test
    public void matchesEqualityStringQueryTermSuccessfully() {
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
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
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
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
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
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
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
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
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
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
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
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
        ADBSelectionQueryTerm term1 = ADBSelectionQueryTerm
                .builder()
                .fieldName("bString")
                .operator(EQUALITY)
                .value("Test")
                .build();

        ADBSelectionQueryTerm term2 = ADBSelectionQueryTerm
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
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
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
        ADBSelectionQueryTerm term = ADBSelectionQueryTerm
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