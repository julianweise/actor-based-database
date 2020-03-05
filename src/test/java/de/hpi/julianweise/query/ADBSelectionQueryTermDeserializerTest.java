package de.hpi.julianweise.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import main.de.hpi.julianweise.csv.TestEntity;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBSelectionQueryTermDeserializerTest {

    @Test
    public void deserializeInteger() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": 4,\n" +
                "  \"fieldName\": \"aInteger\",\n" +
                "  \"operator\": \"EQUALITY\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQuery.SelectionQueryTerm.class, new ADBSelectionQueryTermDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        ADBSelectionQuery.SelectionQueryTerm term = objectMapper.readValue(queryJson, ADBSelectionQuery.SelectionQueryTerm.class);

        assertThat(term.getFieldName()).isEqualTo("aInteger");
        assertThat(term.getValue()).isEqualTo(4);
    }

    @Test
    public void deserializeString() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": \"TestString\",\n" +
                "  \"fieldName\": \"bString\",\n" +
                "  \"operator\": \"EQUALITY\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQuery.SelectionQueryTerm.class, new ADBSelectionQueryTermDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        ADBSelectionQuery.SelectionQueryTerm term = objectMapper.readValue(queryJson, ADBSelectionQuery.SelectionQueryTerm.class);

        assertThat(term.getFieldName()).isEqualTo("bString");
        assertThat(term.getValue()).isEqualTo("TestString");
    }

    @Test
    public void deserializeFloat() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": 1.03,\n" +
                "  \"fieldName\": \"cFloat\",\n" +
                "  \"operator\": \"EQUALITY\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQuery.SelectionQueryTerm.class, new ADBSelectionQueryTermDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        ADBSelectionQuery.SelectionQueryTerm term = objectMapper.readValue(queryJson, ADBSelectionQuery.SelectionQueryTerm.class);

        assertThat(term.getFieldName()).isEqualTo("cFloat");
        assertThat(term.getValue()).isEqualTo(1.03);
    }

    @Test
    public void deserializeBoolean() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": true,\n" +
                "  \"fieldName\": \"dBoolean\",\n" +
                "  \"operator\": \"EQUALITY\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQuery.SelectionQueryTerm.class, new ADBSelectionQueryTermDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        ADBSelectionQuery.SelectionQueryTerm term = objectMapper.readValue(queryJson, ADBSelectionQuery.SelectionQueryTerm.class);

        assertThat(term.getFieldName()).isEqualTo("dBoolean");
        assertThat(term.getValue()).isEqualTo(true);
    }

    @Test
    public void deserializeDouble() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": 12.0374362372462382432,\n" +
                "  \"fieldName\": \"eDouble\",\n" +
                "  \"operator\": \"EQUALITY\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQuery.SelectionQueryTerm.class, new ADBSelectionQueryTermDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        ADBSelectionQuery.SelectionQueryTerm term = objectMapper.readValue(queryJson, ADBSelectionQuery.SelectionQueryTerm.class);

        assertThat(term.getFieldName()).isEqualTo("eDouble");
        assertThat(term.getValue()).isEqualTo(12.0374362372462382432);
    }

    @Test
    public void deserializeChar() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": \"w\",\n" +
                "  \"fieldName\": \"fChar\",\n" +
                "  \"operator\": \"EQUALITY\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQuery.SelectionQueryTerm.class, new ADBSelectionQueryTermDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        ADBSelectionQuery.SelectionQueryTerm term = objectMapper.readValue(queryJson, ADBSelectionQuery.SelectionQueryTerm.class);

        assertThat(term.getFieldName()).isEqualTo("fChar");
        assertThat(term.getValue()).isEqualTo('w');
    }

    @Test(expected = NoSuchFieldException.class)
    public void exceptionDuringDeserialization() throws JsonProcessingException {
        String queryJson = "{\n" +
                "  \"value\": 4,\n" +
                "  \"fieldName\": \"invalidField\",\n" +
                "  \"operator\": \"EQUALITY\"\n" +
                "}";

        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ADBSelectionQuery.SelectionQueryTerm.class, new ADBSelectionQueryTermDeserializer(TestEntity.class));
        objectMapper.registerModule(module);
        objectMapper.readValue(queryJson, ADBSelectionQuery.SelectionQueryTerm.class);
    }

}