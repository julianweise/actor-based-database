package de.hpi.julianweise.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import de.hpi.julianweise.csv.TestEntity;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBSelectionQueryPredicateDeserializerTest {

    @Test
    public void deserializeInteger() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": 4,\n" +
                "  \"fieldName\": \"aInteger\",\n" +
                "  \"operator\": \"EQUALITY\",\n" +
                "  \"@type\": \"ADBSelectionQueryTerm\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQueryPredicate.class, new ADBSelectionQueryTermDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        ADBSelectionQueryPredicate predicate = objectMapper.readValue(queryJson, ADBSelectionQueryPredicate.class);

        assertThat(predicate.getFieldName()).isEqualTo("aInteger");
        assertThat(predicate.getValue()).isEqualTo(4);
    }

    @Test
    public void deserializeString() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": \"TestString\",\n" +
                "  \"fieldName\": \"bString\",\n" +
                "  \"operator\": \"EQUALITY\",\n" +
                "  \"@type\": \"ADBSelectionQueryTerm\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQueryPredicate.class, new ADBSelectionQueryTermDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        ADBSelectionQueryPredicate predicate = objectMapper.readValue(queryJson, ADBSelectionQueryPredicate.class);

        assertThat(predicate.getFieldName()).isEqualTo("bString");
        assertThat(predicate.getValue()).isEqualTo("TestString");
    }

    @Test
    public void deserializeFloat() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": 1.03,\n" +
                "  \"fieldName\": \"cFloat\",\n" +
                "  \"operator\": \"EQUALITY\",\n" +
                "  \"@type\": \"ADBSelectionQueryTerm\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQueryPredicate.class, new ADBSelectionQueryTermDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        ADBSelectionQueryPredicate predicate = objectMapper.readValue(queryJson, ADBSelectionQueryPredicate.class);

        assertThat(predicate.getFieldName()).isEqualTo("cFloat");
        assertThat(predicate.getValue()).isEqualTo(1.03);
    }

    @Test
    public void deserializeBoolean() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": true,\n" +
                "  \"fieldName\": \"dBoolean\",\n" +
                "  \"operator\": \"EQUALITY\",\n" +
                "  \"@type\": \"ADBSelectionQueryTerm\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQueryPredicate.class, new ADBSelectionQueryTermDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        ADBSelectionQueryPredicate predicate = objectMapper.readValue(queryJson, ADBSelectionQueryPredicate.class);

        assertThat(predicate.getFieldName()).isEqualTo("dBoolean");
        assertThat(predicate.getValue()).isEqualTo(true);
    }

    @Test
    public void deserializeDouble() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": 12.0374362372462382432,\n" +
                "  \"fieldName\": \"eDouble\",\n" +
                "  \"operator\": \"EQUALITY\",\n" +
                "  \"@type\": \"ADBSelectionQueryTerm\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQueryPredicate.class, new ADBSelectionQueryTermDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        ADBSelectionQueryPredicate predicate = objectMapper.readValue(queryJson, ADBSelectionQueryPredicate.class);

        assertThat(predicate.getFieldName()).isEqualTo("eDouble");
        assertThat(predicate.getValue()).isEqualTo(12.0374362372462382432);
    }

    @Test
    public void deserializeChar() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": \"w\",\n" +
                "  \"fieldName\": \"fChar\",\n" +
                "  \"operator\": \"EQUALITY\",\n" +
                "  \"@type\": \"ADBSelectionQueryTerm\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQueryPredicate.class, new ADBSelectionQueryTermDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        ADBSelectionQueryPredicate predicate = objectMapper.readValue(queryJson, ADBSelectionQueryPredicate.class);

        assertThat(predicate.getFieldName()).isEqualTo("fChar");
        assertThat(predicate.getValue()).isEqualTo('w');
    }

    @Test(expected = NoSuchFieldException.class)
    public void exceptionDuringDeserialization() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": 4,\n" +
                "  \"fieldName\": \"invalidField\",\n" +
                "  \"operator\": \"EQUALITY\",\n" +
                "  \"@type\": \"ADBSelectionQueryTerm\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQueryPredicate.class, new ADBSelectionQueryTermDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        objectMapper.readValue(queryJson, ADBSelectionQueryPredicate.class);
    }

}