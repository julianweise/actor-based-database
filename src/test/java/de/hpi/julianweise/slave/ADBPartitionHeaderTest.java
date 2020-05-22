package de.hpi.julianweise.slave;

import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.csv.TestEntityFactory;
import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.domain.key.ADBEntityFactoryProvider;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBJoinQueryPredicate;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeader;
import de.hpi.julianweise.slave.partition.meta.ADBPartitionHeaderFactory;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


@SuppressWarnings("ALL") public class ADBPartitionHeaderTest {

    @Before
    public void setUp() {
        ADBEntityFactoryProvider.initialize(new TestEntityFactory());
    }

    @Test
    public void expectCorrectCreation() {
        int partitionId = 1;
        ObjectList<ADBEntity> data = new ObjectArrayList<>();
        data.add(new TestEntity(-1, "T", -1.0f, true, -1.01, 'a'));
        data.add(new TestEntity(1, "Te", 1.0f, false, 1.0, 'b'));
        data.add(new TestEntity(4, "Tes", 1.4f, true, 1.04, 'c'));
        data.add(new TestEntity(8, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader header = ADBPartitionHeaderFactory.createDefault(data, partitionId);

        assertThat(header.getMinValues().size()).isEqualTo(6);
        assertThat(header.getMaxValues().size()).isEqualTo(6);

        assertThat(header.getMinValues().get("aInteger")).isEqualTo(-1);
        assertThat(header.getMaxValues().get("aInteger")).isEqualTo(8);
        assertThat(header.getMinValues().get("bString")).isEqualTo("T");
        assertThat(header.getMaxValues().get("bString")).isEqualTo("Test");
        assertThat(header.getMinValues().get("cFloat")).isEqualTo(-1f);
        assertThat(header.getMaxValues().get("cFloat")).isEqualTo(1.8f);
        assertThat(header.getMinValues().get("dBoolean")).isEqualTo(false);
        assertThat(header.getMaxValues().get("dBoolean")).isEqualTo(true);
        assertThat(header.getMinValues().get("eDouble")).isEqualTo(-1.01);
        assertThat(header.getMaxValues().get("eDouble")).isEqualTo(1.08);
        assertThat(header.getMinValues().get("fChar")).isEqualTo('a');
        assertThat(header.getMaxValues().get("fChar")).isEqualTo('d');
    }

    @Test
    public void expectCorrectOverlappingForLess() {
        int partitionIdLeft = 0;
        int partitionIdRight = 1;

        ObjectList<ADBEntity> dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-1, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(1, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(4, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(8, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft);

        ObjectList<ADBEntity> dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.LESS, "aInteger", "aInteger"));

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-11, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(-21, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(-41, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(-81, "Test", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(22, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(23, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isFalse();
    }

    @Test
    public void expectCorrectOverlappingForLessOrEqual() {
        int partitionIdLeft = 0;
        int partitionIdRight = 1;

        ObjectList<ADBEntity> dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(23, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft);

        ObjectList<ADBEntity> dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.LESS_OR_EQUAL, "aInteger", "aInteger"));

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(24, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isFalse();
    }

    @Test
    public void expectCorrectOverlappingForGreater() {
        int partitionIdLeft = 0;
        int partitionIdRight = 1;

        ObjectList<ADBEntity> dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-1, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(1, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(4, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(8, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft);

        ObjectList<ADBEntity> dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.GREATER, "aInteger", "aInteger"));

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-11, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(-21, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(-41, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(-81, "Test", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isFalse();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(22, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(123, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(131, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(141, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(181, "Test", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();
    }

    @Test
    public void expectCorrectOverlappingForGreaterOrEqual() {
        int partitionIdLeft = 0;
        int partitionIdRight = 1;

        ObjectList<ADBEntity> dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft);

        ObjectList<ADBEntity> dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(23, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.GREATER_OR_EQUAL, "aInteger", "aInteger"));

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(22, "Test", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(23, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));


        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isFalse();
    }

    @Test
    public void expectCorrectOverlappingForEqual() {
        int partitionIdLeft = 0;
        int partitionIdRight = 1;

        ObjectList<ADBEntity> dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft);

        ObjectList<ADBEntity> dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(23, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(22, "Test", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(23, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));


        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isFalse();
    }

    @Test
    public void expectCorrectOverlappingForEqualStrings() {
        int partitionIdLeft = 0;
        int partitionIdRight = 1;

        ObjectList<ADBEntity> dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-5, "T", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(3, "Te", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(15, "Tes", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(23, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft);

        ObjectList<ADBEntity> dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(23, "T", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(31, "Te", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));

        ADBPartitionHeader headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight);

        ADBJoinQuery joinQuery = new ADBJoinQuery();
        joinQuery.addPredicate(new ADBJoinQueryPredicate(ADBQueryTerm.RelationalOperator.EQUALITY, "bString", "bString"));

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isTrue();

        dataLeft = new ObjectArrayList<>();
        dataLeft.add(new TestEntity(-5, "EAsafsdfdA", -1.0f, true, -1.01, 'a'));
        dataLeft.add(new TestEntity(3, "CeEASDasASDAS", 1.0f, false, 1.0, 'b'));
        dataLeft.add(new TestEntity(15, "GRASDASDE", 1.4f, true, 1.04, 'c'));
        dataLeft.add(new TestEntity(22, "Gs#d23$f3QS", 1.8f, false, 1.08, 'd'));

        headerLeft = ADBPartitionHeaderFactory.createDefault(dataLeft, partitionIdLeft);

        dataRight = new ObjectArrayList<>();
        dataRight.add(new TestEntity(23, "T1", -1.0f, true, -1.01, 'a'));
        dataRight.add(new TestEntity(31, "T2", 1.0f, false, 1.0, 'b'));
        dataRight.add(new TestEntity(41, "Tes", 1.4f, true, 1.04, 'c'));
        dataRight.add(new TestEntity(81, "Test", 1.8f, false, 1.08, 'd'));


        headerRight = ADBPartitionHeaderFactory.createDefault(dataRight, partitionIdRight);

        assertThat(headerLeft.isOverlapping(headerRight, joinQuery)).isFalse();
    }

}