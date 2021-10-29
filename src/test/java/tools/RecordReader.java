package tools;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.util.HashMap;
import java.util.Map;

public class RecordReader {
    private static Schema keySchema;
    private static Schema valueSchema;
    private static JsonConverter jsonConverter;

    static {
        keySchema = SchemaBuilder.struct()
                .field("pk0", Schema.STRING_SCHEMA)
                .build();
        valueSchema= SchemaBuilder.struct()
                .field("A", Schema.OPTIONAL_STRING_SCHEMA)
                .field("B", Schema.OPTIONAL_INT64_SCHEMA)
                .field("C", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .build();

        //JsonConverter
        Map<String, String> jsonProps = new HashMap<>();
        jsonProps.put(JsonConverterConfig.TYPE_CONFIG, "value");
        jsonProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        jsonConverter = new JsonConverter();
        jsonConverter.configure(jsonProps);
    }

    public static ProducerRecord<byte[], byte[]> getRecord(String topic, long index) {
        Struct key=new Struct(keySchema)
                .put("pk0",String.valueOf(index));
        Struct value=new Struct(valueSchema)
                .put("A", "test"+index)
                .put("B", index)
                .put("C",(double)index);

        byte[] keyBytes=jsonConverter.fromConnectData(topic,keySchema,key);
        byte[] valueBytes=jsonConverter.fromConnectData(topic,valueSchema,value);
        ProducerRecord<byte[], byte[]> record=new ProducerRecord<byte[], byte[]>(topic, null, System.currentTimeMillis(), keyBytes, valueBytes);
        return record;
    }
}
