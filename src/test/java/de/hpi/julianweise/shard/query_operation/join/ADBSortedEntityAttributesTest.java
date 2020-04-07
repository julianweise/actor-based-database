package de.hpi.julianweise.shard.query_operation.join;

import de.hpi.julianweise.domain.ADBEntityType;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.csv.TestEntityFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ADBSortedEntityAttributesTest {

    @Before
    public void setUp() {
        ADBEntityFactoryProvider.initialize(new TestEntityFactory());
    }

    @Test
    public void emptySortedEntityAttributesCollection() {
        List<ADBEntityType> entities = new ArrayList<>();
        ADBSortedEntityAttributes attributes = ADBSortedEntityAttributes.of("aInteger", entities);

        assertThat(attributes.size()).isZero();
        assertThat(attributes.getAllWithOriginalIndex().size()).isZero();
        assertThat(attributes.iterator().hasNext()).isFalse();
    }

    @Test
    public void valuesAreSorted() {
        List<ADBEntityType> entities = new ArrayList<>();
        entities.add(new TestEntity(5, "Test", 1f, true, 1.01, 'a'));
        entities.add(new TestEntity(1, "Test", 1f, true, 1.01, 'a'));
        entities.add(new TestEntity(23, "Test", 1f, true, 1.01, 'a'));
        entities.add(new TestEntity(4, "Test", 1f, true, 1.01, 'a'));

        ADBSortedEntityAttributes attributes = ADBSortedEntityAttributes.of("aInteger", entities);

        assertThat(attributes.size()).isEqualTo(4);
        assertThat(attributes.getAllWithOriginalIndex().size()).isEqualTo(4);

        assertThat((attributes.get(0))).isEqualTo(1);
        assertThat((attributes.get(1))).isEqualTo(4);
        assertThat((attributes.get(2))).isEqualTo(5);
        assertThat((attributes.get(3))).isEqualTo(23);

        assertThat(attributes.iterator().hasNext()).isTrue();
        assertThat(attributes.iterator().next()).isEqualTo(1);

        assertThat((attributes.getWithOriginalIndex(1).getKey())).isEqualTo(1);
        assertThat((attributes.getWithOriginalIndex(1).getValue())).isEqualTo(1);
        assertThat((attributes.getWithOriginalIndex(3).getKey())).isEqualTo(4);
        assertThat((attributes.getWithOriginalIndex(3).getValue())).isEqualTo(3);
        assertThat((attributes.getWithOriginalIndex(0).getKey())).isEqualTo(5);
        assertThat((attributes.getWithOriginalIndex(0).getValue())).isEqualTo(0);
        assertThat((attributes.getWithOriginalIndex(2).getKey())).isEqualTo(23);
        assertThat((attributes.getWithOriginalIndex(2).getValue())).isEqualTo(2);

        assertThat(attributes.getAllWithOriginalIndex().get(0).getKey()).isEqualTo(1);
        assertThat(attributes.getAllWithOriginalIndex().get(1).getKey()).isEqualTo(4);
        assertThat(attributes.getAllWithOriginalIndex().get(2).getKey()).isEqualTo(5);
        assertThat(attributes.getAllWithOriginalIndex().get(3).getKey()).isEqualTo(23);
    }

    @Test
    public void valuesExtractedFromQueryAreSorted() {
        List<ADBEntityType> entities = new ArrayList<>();
        entities.add(new TestEntity(5, "Test2", 1.0f, true, 1.01, 'a'));
        entities.add(new TestEntity(1, "Test55", 0.3f, true, 1.01, 'a'));
        entities.add(new TestEntity(23, "Test", 20.2f, true, 1.01, 'a'));
        entities.add(new TestEntity(4, "Test0", 3f, true, 1.01, 'a'));

        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "bString"));
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "cFloat"));

        Map<String, ADBSortedEntityAttributes> attributes = ADBSortedEntityAttributes.of(query, entities);

        assertThat(attributes.size()).isEqualTo(3);
        assertThat(attributes.containsKey("aInteger"));
        assertThat(attributes.containsKey("bString"));
        assertThat(attributes.containsKey("cFloat"));

        assertThat(attributes.get("aInteger").size()).isEqualTo(4);
        assertThat(attributes.get("aInteger").getAllWithOriginalIndex().size()).isEqualTo(4);

        assertThat((attributes.get("aInteger").get(0))).isEqualTo(1);
        assertThat((attributes.get("aInteger").get(1))).isEqualTo(4);
        assertThat((attributes.get("aInteger").get(2))).isEqualTo(5);
        assertThat((attributes.get("aInteger").get(3))).isEqualTo(23);

        assertThat(attributes.get("bString").size()).isEqualTo(4);
        assertThat(attributes.get("bString").getAllWithOriginalIndex().size()).isEqualTo(4);

        assertThat((attributes.get("bString").get(0))).isEqualTo("Test");
        assertThat((attributes.get("bString").get(1))).isEqualTo("Test0");
        assertThat((attributes.get("bString").get(2))).isEqualTo("Test2");
        assertThat((attributes.get("bString").get(3))).isEqualTo("Test55");

        assertThat(attributes.get("cFloat").size()).isEqualTo(4);
        assertThat(attributes.get("cFloat").getAllWithOriginalIndex().size()).isEqualTo(4);

        assertThat((attributes.get("cFloat").get(0))).isEqualTo(0.3f);
        assertThat((attributes.get("cFloat").get(1))).isEqualTo(1.0f);
        assertThat((attributes.get("cFloat").get(2))).isEqualTo(3f);
        assertThat((attributes.get("cFloat").get(3))).isEqualTo(20.2f);
    }
}