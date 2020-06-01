package de.hpi.julianweise.slave;

import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.csv.TestEntityFactory;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.query.join.ADBJoinQuery;
import de.hpi.julianweise.query.join.ADBJoinQueryPredicate;
import de.hpi.julianweise.slave.partition.data.ADBEntity;
import de.hpi.julianweise.slave.partition.data.comparator.ADBComparator;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityBooleanEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityDoubleEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityEntryFactory;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityFloatEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityIntEntry;
import de.hpi.julianweise.slave.partition.data.entry.ADBEntityStringEntry;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeader;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeaderFactory;
import de.hpi.julianweise.slave.partition.meta.ADBSortedEntityAttributes;
import de.hpi.julianweise.slave.partition.meta.ADBSortedEntityAttributesFactory;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("SpellCheckingInspection")
public class ADBPartitionHeaderTest {

    @Before
    public void setUp() {
        ADBEntityFactoryProvider.initialize(new TestEntityFactory());
        ADBComparator.buildComparatorMapping();
    }

    @Test
    public void expectCorrectCreation() {
        int partitionId = 1;
        ObjectList<ADBEntity> data = new ObjectArrayList<>();
        data.add(new TestEntity(-1, "T", -1.0f, true, -1.01));
        data.add(new TestEntity(1, "Te", 1.0f, false, 1.0));
        data.add(new TestEntity(4, "Tes", 1.4f, true, 1.04));
        data.add(new TestEntity(8, "Test", 1.8f, false, 1.08));

        Map<String, ADBSortedEntityAttributes> sortedAttributesL = ADBSortedEntityAttributesFactory.of(data);
        ADBPartitionHeader header = ADBPartitionHeaderFactory.createDefault(data, partitionId, sortedAttributesL);

        assertThat(header.getMinValues().size()).isEqualTo(5);
        assertThat(header.getMaxValues().size()).isEqualTo(5);

        assertThat(((ADBEntityIntEntry)header.getMinValues().get("aInteger")).value).isEqualTo(-1);
        assertThat(((ADBEntityIntEntry)header.getMaxValues().get("aInteger")).value).isEqualTo(8);
        assertThat(((ADBEntityStringEntry)header.getMinValues().get("bString")).value).isEqualTo("T");
        assertThat(((ADBEntityStringEntry)header.getMaxValues().get("bString")).value).isEqualTo("Test");
        assertThat(((ADBEntityFloatEntry)header.getMinValues().get("cFloat")).value).isEqualTo(-1f);
        assertThat(((ADBEntityFloatEntry)header.getMaxValues().get("cFloat")).value).isEqualTo(1.8f);
        assertThat(((ADBEntityBooleanEntry)header.getMinValues().get("dBoolean")).value).isEqualTo(false);
        assertThat(((ADBEntityBooleanEntry)header.getMaxValues().get("dBoolean")).value).isEqualTo(true);
        assertThat(((ADBEntityDoubleEntry)header.getMinValues().get("eDouble")).value).isEqualTo(-1.01);
        assertThat(((ADBEntityDoubleEntry)header.getMaxValues().get("eDouble")).value).isEqualTo(1.08);
    }

    @Test
    public void expectCorrectOverlappingForLess() {
        int partitionIdLeft = 0;
        int partitionIdRight = 1;

        ObjectList<ADBEntity> dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-1, "T", -1.0f, true, -1.01));
        dataLeft.add(new TestEntity(1, "Te", 1.0f, false, 1.0));
        dataLeft.add(new TestEntity(4, "Tes", 1.4f, true, 1.04));
        dataLeft.add(new TestEntity(8, "Test", 1.8f, false, 1.08));

        Map<String, ADBSortedEntityAttributes> sortedAttributesL = ADBSortedEntityAttributesFactory.of(dataLeft);
        ADBPartitionHeader headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft, sortedAttributesL);

        ObjectList<ADBEntity> dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08));

        Map<String, ADBSortedEntityAttributes> sortedAttributesR = ADBSortedEntityAttributesFactory.of(dataLeft);


        ADBPartitionHeader headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight, sortedAttributesR);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.LESS, "aInteger", "aInteger"));

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-11, "T", -1.0f, true, -1.01));
        dataLeft.add(new TestEntity(-21, "Te", 1.0f, false, 1.0));
        dataLeft.add(new TestEntity(-41, "Tes", 1.4f, true, 1.04));
        dataLeft.add(new TestEntity(-81, "Test", 1.8f, false, 1.08));

        sortedAttributesL = ADBSortedEntityAttributesFactory.of(dataLeft);
        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft, sortedAttributesL);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08));

        sortedAttributesR = ADBSortedEntityAttributesFactory.of(dataRight);
        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight, sortedAttributesR);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(22, "T", -1.0f, true, -1.01));
        dataLeft.add(new TestEntity(31, "Te", 1.0f, false, 1.0));
        dataLeft.add(new TestEntity(41, "Tes", 1.4f, true, 1.04));
        dataLeft.add(new TestEntity(81, "Test", 1.8f, false, 1.08));

        sortedAttributesL = ADBSortedEntityAttributesFactory.of(dataLeft);
        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft, sortedAttributesL);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08));

        sortedAttributesR = ADBSortedEntityAttributesFactory.of(dataRight);
        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight, sortedAttributesR);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(23, "T", -1.0f, true, -1.01));
        dataLeft.add(new TestEntity(31, "Te", 1.0f, false, 1.0));
        dataLeft.add(new TestEntity(41, "Tes", 1.4f, true, 1.04));
        dataLeft.add(new TestEntity(81, "Test", 1.8f, false, 1.08));

        sortedAttributesL = ADBSortedEntityAttributesFactory.of(dataLeft);
        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft, sortedAttributesL);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08));

        sortedAttributesR = ADBSortedEntityAttributesFactory.of(dataRight);
        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight, sortedAttributesR);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isFalse();
    }

    @Test
    public void expectCorrectOverlappingForLessOrEqual() {
        int partitionIdLeft = 0;
        int partitionIdRight = 1;

        ObjectList<ADBEntity> dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(23, "T", -1.0f, true, -1.01));
        dataLeft.add(new TestEntity(31, "Te", 1.0f, false, 1.0));
        dataLeft.add(new TestEntity(41, "Tes", 1.4f, true, 1.04));
        dataLeft.add(new TestEntity(81, "Test", 1.8f, false, 1.08));

        Map<String, ADBSortedEntityAttributes> sortedAttributesL = ADBSortedEntityAttributesFactory.of(dataLeft);
        ADBPartitionHeader headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft, sortedAttributesL);

        ObjectList<ADBEntity> dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08));

        Map<String, ADBSortedEntityAttributes> sortedAttributesR = ADBSortedEntityAttributesFactory.of(dataRight);
        ADBPartitionHeader headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight, sortedAttributesR);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.LESS_OR_EQUAL, "aInteger", "aInteger"));

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(24, "T", -1.0f, true, -1.01));
        dataLeft.add(new TestEntity(31, "Te", 1.0f, false, 1.0));
        dataLeft.add(new TestEntity(41, "Tes", 1.4f, true, 1.04));
        dataLeft.add(new TestEntity(81, "Test", 1.8f, false, 1.08));

        sortedAttributesL = ADBSortedEntityAttributesFactory.of(dataLeft);
        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft, sortedAttributesL);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08));

        sortedAttributesR = ADBSortedEntityAttributesFactory.of(dataRight);
        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight, sortedAttributesR);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isFalse();
    }

    @Test
    public void expectCorrectOverlappingForGreater() {
        int partitionIdLeft = 0;
        int partitionIdRight = 1;

        ObjectList<ADBEntity> dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-1, "T", -1.0f, true, -1.01));
        dataLeft.add(new TestEntity(1, "Te", 1.0f, false, 1.0));
        dataLeft.add(new TestEntity(4, "Tes", 1.4f, true, 1.04));
        dataLeft.add(new TestEntity(8, "Test", 1.8f, false, 1.08));

        Map<String, ADBSortedEntityAttributes> sortedAttributesL = ADBSortedEntityAttributesFactory.of(dataLeft);
        ADBPartitionHeader headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft, sortedAttributesL);

        ObjectList<ADBEntity> dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08));

        Map<String, ADBSortedEntityAttributes> sortedAttributesR = ADBSortedEntityAttributesFactory.of(dataRight);
        ADBPartitionHeader headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight, sortedAttributesR);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.GREATER, "aInteger", "aInteger"));

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-11, "T", -1.0f, true, -1.01));
        dataLeft.add(new TestEntity(-21, "Te", 1.0f, false, 1.0));
        dataLeft.add(new TestEntity(-41, "Tes", 1.4f, true, 1.04));
        dataLeft.add(new TestEntity(-81, "Test", 1.8f, false, 1.08));

        sortedAttributesL = ADBSortedEntityAttributesFactory.of(dataLeft);
        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft, sortedAttributesL);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08));

        sortedAttributesR = ADBSortedEntityAttributesFactory.of(dataRight);
        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight, sortedAttributesR);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isFalse();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(22, "T", -1.0f, true, -1.01));
        dataLeft.add(new TestEntity(31, "Te", 1.0f, false, 1.0));
        dataLeft.add(new TestEntity(41, "Tes", 1.4f, true, 1.04));
        dataLeft.add(new TestEntity(81, "Test", 1.8f, false, 1.08));

        sortedAttributesL = ADBSortedEntityAttributesFactory.of(dataLeft);
        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft, sortedAttributesL);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08));

        sortedAttributesR = ADBSortedEntityAttributesFactory.of(dataRight);
        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight, sortedAttributesR);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(123, "T", -1.0f, true, -1.01));
        dataLeft.add(new TestEntity(131, "Te", 1.0f, false, 1.0));
        dataLeft.add(new TestEntity(141, "Tes", 1.4f, true, 1.04));
        dataLeft.add(new TestEntity(181, "Test", 1.8f, false, 1.08));

        sortedAttributesL = ADBSortedEntityAttributesFactory.of(dataLeft);
        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft, sortedAttributesL);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08));

        sortedAttributesR = ADBSortedEntityAttributesFactory.of(dataRight);
        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight, sortedAttributesR);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();
    }

    @Test
    public void expectCorrectOverlappingForGreaterOrEqual() {
        int partitionIdLeft = 0;
        int partitionIdRight = 1;

        ObjectList<ADBEntity> dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-5, "T", -1.0f, true, -1.01));
        dataLeft.add(new TestEntity(3, "Te", 1.0f, false, 1.0));
        dataLeft.add(new TestEntity(15, "Tes", 1.4f, true, 1.04));
        dataLeft.add(new TestEntity(23, "Test", 1.8f, false, 1.08));

        Map<String, ADBSortedEntityAttributes> sortedAttributesL = ADBSortedEntityAttributesFactory.of(dataLeft);
        ADBPartitionHeader headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft, sortedAttributesL);

        ObjectList<ADBEntity> dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(23, "T", -1.0f, true, -1.01));
        dataRight.add(new TestEntity(31, "Te", 1.0f, false, 1.0));
        dataRight.add(new TestEntity(41, "Tes", 1.4f, true, 1.04));
        dataRight.add(new TestEntity(81, "Test", 1.8f, false, 1.08));

        Map<String, ADBSortedEntityAttributes> sortedAttributesR = ADBSortedEntityAttributesFactory.of(dataRight);
        ADBPartitionHeader headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight, sortedAttributesR);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL, "aInteger", "aInteger"));

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-5, "T", -1.0f, true, -1.01));
        dataLeft.add(new TestEntity(3, "Te", 1.0f, false, 1.0));
        dataLeft.add(new TestEntity(15, "Tes", 1.4f, true, 1.04));
        dataLeft.add(new TestEntity(22, "Test", 1.8f, false, 1.08));

        sortedAttributesL = ADBSortedEntityAttributesFactory.of(dataLeft);
        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft, sortedAttributesL);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(23, "T", -1.0f, true, -1.01));
        dataRight.add(new TestEntity(31, "Te", 1.0f, false, 1.0));
        dataRight.add(new TestEntity(41, "Tes", 1.4f, true, 1.04));
        dataRight.add(new TestEntity(81, "Test", 1.8f, false, 1.08));

        sortedAttributesR = ADBSortedEntityAttributesFactory.of(dataRight);
        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight, sortedAttributesR);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isFalse();
    }

    @Test
    public void expectCorrectOverlappingForEqual() {
        int partitionIdLeft = 0;
        int partitionIdRight = 1;

        ObjectList<ADBEntity> dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-5, "T", -1.0f, true, -1.01));
        dataLeft.add(new TestEntity(3, "Te", 1.0f, false, 1.0));
        dataLeft.add(new TestEntity(15, "Tes", 1.4f, true, 1.04));
        dataLeft.add(new TestEntity(23, "Test", 1.8f, false, 1.08));

        Map<String, ADBSortedEntityAttributes> sortedAttributesL = ADBSortedEntityAttributesFactory.of(dataLeft);
        ADBPartitionHeader headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft, sortedAttributesL);

        ObjectList<ADBEntity> dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(23, "T", -1.0f, true, -1.01));
        dataRight.add(new TestEntity(31, "Te", 1.0f, false, 1.0));
        dataRight.add(new TestEntity(41, "Tes", 1.4f, true, 1.04));
        dataRight.add(new TestEntity(81, "Test", 1.8f, false, 1.08));

        Map<String, ADBSortedEntityAttributes> sortedAttributesR = ADBSortedEntityAttributesFactory.of(dataRight);
        ADBPartitionHeader headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight, sortedAttributesR);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-5, "T", -1.0f, true, -1.01));
        dataLeft.add(new TestEntity(3, "Te", 1.0f, false, 1.0));
        dataLeft.add(new TestEntity(15, "Tes", 1.4f, true, 1.04));
        dataLeft.add(new TestEntity(22, "Test", 1.8f, false, 1.08));

        sortedAttributesL = ADBSortedEntityAttributesFactory.of(dataLeft);
        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft, sortedAttributesL);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(23, "T", -1.0f, true, -1.01));
        dataRight.add(new TestEntity(31, "Te", 1.0f, false, 1.0));
        dataRight.add(new TestEntity(41, "Tes", 1.4f, true, 1.04));
        dataRight.add(new TestEntity(81, "Test", 1.8f, false, 1.08));

        sortedAttributesR = ADBSortedEntityAttributesFactory.of(dataRight);
        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight, sortedAttributesR);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isFalse();
    }

    @Test
    public void expectCorrectOverlappingForEqualStrings() {
        int partitionIdLeft = 0;
        int partitionIdRight = 1;

        ObjectList<ADBEntity> dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-5, "T", -1.0f, true, -1.01));
        dataLeft.add(new TestEntity(3, "Te", 1.0f, false, 1.0));
        dataLeft.add(new TestEntity(15, "Tes", 1.4f, true, 1.04));
        dataLeft.add(new TestEntity(23, "Test", 1.8f, false, 1.08));

        Map<String, ADBSortedEntityAttributes> sortedAttributesL = ADBSortedEntityAttributesFactory.of(dataLeft);
        ADBPartitionHeader headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft, sortedAttributesL);

        ObjectList<ADBEntity> dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(23, "T", -1.0f, true, -1.01));
        dataRight.add(new TestEntity(31, "Te", 1.0f, false, 1.0));
        dataRight.add(new TestEntity(41, "Tes", 1.4f, true, 1.04));
        dataRight.add(new TestEntity(81, "Test", 1.8f, false, 1.08));

        Map<String, ADBSortedEntityAttributes> sortedAttributesR = ADBSortedEntityAttributesFactory.of(dataRight);
        ADBPartitionHeader headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight, sortedAttributesR);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "bString", "bString"));

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-5, "EAsafsdfdA", -1.0f, true, -1.01));
        dataLeft.add(new TestEntity(3, "CeEASDasASDAS", 1.0f, false, 1.0));
        dataLeft.add(new TestEntity(15, "GRASDASDE", 1.4f, true, 1.04));
        dataLeft.add(new TestEntity(22, "Gs#d23$f3QS", 1.8f, false, 1.08));

        sortedAttributesL = ADBSortedEntityAttributesFactory.of(dataLeft);
        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft, sortedAttributesL);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(23, "T1", -1.0f, true, -1.01));
        dataRight.add(new TestEntity(31, "T2", 1.0f, false, 1.0));
        dataRight.add(new TestEntity(41, "Tes", 1.4f, true, 1.04));
        dataRight.add(new TestEntity(81, "Test", 1.8f, false, 1.08));

        sortedAttributesR = ADBSortedEntityAttributesFactory.of(dataRight);
        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight, sortedAttributesR);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isFalse();
    }

}