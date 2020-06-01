package de.hpi.julianweise.shard;

import de.hpi.julianweise.query.selection.ADBSelectionQuery;
import de.hpi.julianweise.query.selection.ADBSelectionQueryPredicate;
import de.hpi.julianweise.query.selection.constant.ADBPredicateIntConstant;
import org.junit.Test;

import static de.hpi.julianweise.query.ADBQueryTerm.RelationalOperator.EQUALITY;
import static org.assertj.core.api.Assertions.assertThat;

public class ADBSelectionQueryTest {

    @Test
    public void addTermToQuery() {
        ADBSelectionQuery query = new ADBSelectionQuery();

        assertThat(query.getPredicates().size()).isZero();

        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate
                .builder()
                .fieldName("aInteger")
                .operator(EQUALITY)
                .value(new ADBPredicateIntConstant(2))
                .build();
        query.addPredicate(predicate);

        assertThat(query.getPredicates().size()).isEqualTo(1);
        assertThat(query.getPredicates().get(0)).isEqualTo(predicate);
    }

    @Test
    public void queryTermStringRepresentation() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate.builder()
                                                                    .fieldName("aInteger")
                                                                    .operator(EQUALITY)
                                                                    .value(new ADBPredicateIntConstant(2))
                                                                    .build();

        assertThat(predicate.toString()).contains("Predicate");
        assertThat(predicate.toString()).contains("2");
        assertThat(predicate.toString()).contains("aInteger");
        assertThat(predicate.toString()).contains("EQUALITY");
    }

    @Test
    public void queryStringRepresentation() {
        ADBSelectionQueryPredicate predicate = ADBSelectionQueryPredicate.builder()
                                                                    .fieldName("aInteger")
                                                                    .operator(EQUALITY)
                                                                    .value(new ADBPredicateIntConstant(2))
                                                                    .build();

        ADBSelectionQuery query = new ADBSelectionQuery();
        query.addPredicate(predicate);

        assertThat(query.toString()).contains("Query");
        assertThat(query.toString()).contains("2");
        assertThat(query.toString()).contains("aInteger");
        assertThat(query.toString()).contains("EQUALITY");
    }

    @Test
    public void noArgsConstructorIsPresentForDeserialization() {
        ADBSelectionQueryPredicate predicate = new ADBSelectionQueryPredicate();

        assertThat(predicate.getFieldName()).isNull();
        assertThat(predicate.getValue()).isNull();
        assertThat(predicate.getOperator()).isNull();
    }

}