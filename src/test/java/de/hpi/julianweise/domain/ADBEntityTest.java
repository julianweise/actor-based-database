package de.hpi.julianweise.domain;

import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.query.selection.ADBSelectionQuery;
import de.hpi.julianweise.query.selection.ADBSelectionQueryPredicate;
import de.hpi.julianweise.query.selection.constant.ADBPredicateBooleanConstant;
import de.hpi.julianweise.query.selection.constant.ADBPredicateFloatConstant;
import de.hpi.julianweise.query.selection.constant.ADBPredicateIntConstant;
import de.hpi.julianweise.query.selection.constant.ADBPredicateStringConstant;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntryFactory;
import org.junit.Before;
import org.junit.Test;

import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.EQUALITY;
import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.GREATER;
import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL;
import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.INEQUALITY;
import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.LESS;
import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.LESS_OR_EQUAL;
import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.UNSPECIFIED;
import static org.assertj.core.api.Assertions.assertThat;

public class ADBEntityTest {

    @Before
    public void setUp() {
        ADBComparator.buildComparatorMapping();
        ADBComparator.buildComparatorMapping();
    }

    // ##### Integer #####
    @Test
    public void matchesEqualityIntegerQueryTermSuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("aInteger")
                .operator(EQUALITY)
                .value(new ADBPredicateIntConstant(1))
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity.matches(predicate)).isTrue();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232);
        assertThat(entity2.matches(predicate)).isFalse();
    }

    @Test
    public void matchesInEqualityIntegerQueryTermSuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("aInteger")
                .operator(INEQUALITY)
                .value(new ADBPredicateIntConstant(1))
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity.matches(predicate)).isFalse();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232);
        assertThat(entity2.matches(predicate)).isTrue();
    }

    @Test
    public void matchesLessIntegerQueryTermSuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("aInteger")
                .operator(LESS)
                .value(new ADBPredicateIntConstant(1))
                .build();

        TestEntity entity = new TestEntity(2, "Test", 1.01f, true, 12.94232);
        assertThat(entity.matches(predicate)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity1.matches(predicate)).isFalse();

        TestEntity entity2 = new TestEntity(0, "Test", 1.01f, true, 12.94232);
        assertThat(entity2.matches(predicate)).isTrue();
    }

    @Test
    public void matchesGreaterIntegerQueryTermSuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("aInteger")
                .operator(GREATER)
                .value(new ADBPredicateIntConstant(1))
                .build();

        TestEntity entity = new TestEntity(2, "Test", 1.01f, true, 12.94232);
        assertThat(entity.matches(predicate)).isTrue();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity1.matches(predicate)).isFalse();

        TestEntity entity2 = new TestEntity(0, "Test", 1.01f, true, 12.94232);
        assertThat(entity2.matches(predicate)).isFalse();
    }

    @Test
    public void matchesLessOrEqualIntegerQueryTermSuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("aInteger")
                .operator(LESS_OR_EQUAL)
                .value(new ADBPredicateIntConstant(1))
                .build();

        TestEntity entity = new TestEntity(0, "Test", 1.01f, true, 12.94232);
        assertThat(entity.matches(predicate)).isTrue();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity1.matches(predicate)).isTrue();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232);
        assertThat(entity2.matches(predicate)).isFalse();
    }

    @Test
    public void matchesGreaterOrEqualIntegerQueryTermSuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("aInteger")
                .operator(GREATER_OR_EQUAL)
                .value(new ADBPredicateIntConstant(1))
                .build();

        TestEntity entity = new TestEntity(0, "Test", 1.01f, true, 12.94232);
        assertThat(entity.matches(predicate)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity1.matches(predicate)).isTrue();

        TestEntity entity2 = new TestEntity(2, "Test", 1.01f, true, 12.94232);
        assertThat(entity2.matches(predicate)).isTrue();
    }

    // ##### Float #####

    @Test
    public void matchesEqualityFloatQueryTermSuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("cFloat")
                .operator(EQUALITY)
                .value(new ADBPredicateFloatConstant(1.01f))
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity.matches(predicate)).isTrue();

        TestEntity entity2 = new TestEntity(1, "Test", 1.00f, true, 12.94232);
        assertThat(entity2.matches(predicate)).isFalse();
    }

    @Test
    public void matchesInEqualityFloatQueryTermSuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("cFloat")
                .operator(INEQUALITY)
                .value(new ADBPredicateFloatConstant(1.01f))
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity.matches(predicate)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 1.00f, true, 12.94232);
        assertThat(entity2.matches(predicate)).isTrue();
    }

    @Test
    public void matchesLessFloatQueryTermSuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("cFloat")
                .operator(LESS)
                .value(new ADBPredicateFloatConstant(1.01f))
                .build();

        TestEntity entity = new TestEntity(1, "Test", 2.00f, true, 12.94232);
        assertThat(entity.matches(predicate)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 2.01f, true, 12.02);
        assertThat(entity1.matches(predicate)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 0.01f, true, 12.02);
        assertThat(entity2.matches(predicate)).isTrue();
    }

    @Test
    public void matchesGreaterFloatQueryTermSuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("cFloat")
                .operator(GREATER)
                .value(new ADBPredicateFloatConstant(1.01f))
                .build();

        TestEntity entity = new TestEntity(1, "Test", 2.00f, true, 12.94232);
        assertThat(entity.matches(predicate)).isTrue();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity1.matches(predicate)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 1.00f, true, 12.02);
        assertThat(entity2.matches(predicate)).isFalse();
    }

    @Test
    public void matchesLessOrEqualFloatQueryTermSuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("cFloat")
                .operator(LESS_OR_EQUAL)
                .value(new ADBPredicateFloatConstant(1.01f))
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.00f, true, 12.94232);
        assertThat(entity.matches(predicate)).isTrue();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity1.matches(predicate)).isTrue();

        TestEntity entity2 = new TestEntity(1, "Test", 1.02f, true, 12.02);
        assertThat(entity2.matches(predicate)).isFalse();
    }

    @Test
    public void matchesGreaterOrEqualFloatQueryTermSuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("cFloat")
                .operator(GREATER_OR_EQUAL)
                .value(new ADBPredicateFloatConstant(1.01f))
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.00f, true, 12.94232);
        assertThat(entity.matches(predicate)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity1.matches(predicate)).isTrue();

        TestEntity entity2 = new TestEntity(1, "Test", 2.01f, true, 12.02);
        assertThat(entity2.matches(predicate)).isTrue();
    }

    // ##### String #####

    @Test
    public void matchesEqualityStringQueryTermSuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("bString")
                .operator(EQUALITY)
                .value(new ADBPredicateStringConstant("Test"))
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity.matches(predicate)).isTrue();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, true, 12.02);
        assertThat(entity2.matches(predicate)).isFalse();
    }

    @Test
    public void matchesInEqualityStringQueryTermSuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("bString")
                .operator(INEQUALITY)
                .value(new ADBPredicateStringConstant("Test"))
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity.matches(predicate)).isFalse();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, true, 12.02);
        assertThat(entity2.matches(predicate)).isTrue();
    }

    // ##### Boolean #####

    @Test
    public void matchesEqualityBooleanQueryTermSuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("dBoolean")
                .operator(EQUALITY)
                .value(new ADBPredicateBooleanConstant(true))
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity.matches(predicate)).isTrue();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, false, 12.02);
        assertThat(entity2.matches(predicate)).isFalse();
    }

    @Test
    public void matchesInEqualityBooleanQueryTermSuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("dBoolean")
                .operator(INEQUALITY)
                .value(new ADBPredicateBooleanConstant(true))
                .build();

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity.matches(predicate)).isFalse();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, false, 12.02);
        assertThat(entity2.matches(predicate)).isTrue();
    }

    // #### Defaults / Exceptions

    @Test
    public void defaultsToFalseForUnknownOperator() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("cFloat")
                .operator(UNSPECIFIED)
                .value(new ADBPredicateFloatConstant(1.01f))
                .build();


        TestEntity entity = new TestEntity(1, "Test", 1.00f, true, 12.94232);
        assertThat(entity.matches(predicate)).isFalse();

        TestEntity entity1 = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity1.matches(predicate)).isFalse();

        TestEntity entity2 = new TestEntity(1, "Test", 2.01f, true, 12.02);
        assertThat(entity2.matches(predicate)).isFalse();
    }

    // #### Queries ####

    @Test
    public void matchesStringQuerySuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("bString")
                .operator(EQUALITY)
                .value(new ADBPredicateStringConstant("Test"))
                .build();

        ADBSelectionQuery query = new ADBSelectionQuery();
        query.addPredicate(predicate);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity.matches(query)).isTrue();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, true, 12.02);
        assertThat(entity2.matches(query)).isFalse();
    }

    @Test
    public void matchesQueryMultipleTermsSuccessfully() {
        ADBSelectionQueryPredicate term1 = ADBSelectionQueryPredicate
                .builder()
                .fieldName("bString")
                .operator(EQUALITY)
                .value(new ADBPredicateStringConstant("Test"))
                .build();

        ADBSelectionQueryPredicate term2 = ADBSelectionQueryPredicate
                .builder()
                .fieldName("cFloat")
                .operator(INEQUALITY)
                .value(new ADBPredicateFloatConstant(1f))
                .build();

        ADBSelectionQuery query = new ADBSelectionQuery();
        query.addPredicate(term1);
        query.addPredicate(term2);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity.matches(query)).isTrue();

        TestEntity entity2 = new TestEntity(1, "TestNotEqual", 1.00f, true, 12.02);
        assertThat(entity2.matches(query)).isFalse();
    }

    @Test
    public void matchesIntegerQuerySuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("aInteger")
                .operator(EQUALITY)
                .value(new ADBPredicateIntConstant(1))
                .build();

        ADBSelectionQuery query = new ADBSelectionQuery();
        query.addPredicate(predicate);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity.matches(query)).isTrue();

        TestEntity entity2 = new TestEntity(2, "TestNotEqual", 1.00f, true, 12.02);
        assertThat(entity2.matches(query)).isFalse();
    }

    @Test
    public void matchesFloatQuerySuccessfully() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("cFloat")
                .operator(EQUALITY)
                .value(new ADBPredicateFloatConstant(1.01f))
                .build();

        ADBSelectionQuery query = new ADBSelectionQuery();
        query.addPredicate(predicate);

        TestEntity entity = new TestEntity(1, "Test", 1.01f, true, 12.02);
        assertThat(entity.matches(query)).isTrue();

        TestEntity entity2 = new TestEntity(2, "TestNotEqual", 1.00f, true, 12.02);
        assertThat(entity2.matches(query)).isFalse();
    }
}