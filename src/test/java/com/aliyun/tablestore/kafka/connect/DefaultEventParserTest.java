package com.aliyun.tablestore.kafka.connect;

import com.alicloud.openservices.tablestore.core.utils.Bytes;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.tablestore.kafka.connect.parsers.DefaultEventParser;
import com.aliyun.tablestore.kafka.connect.parsers.EventParser;
import com.aliyun.tablestore.kafka.connect.parsers.EventParsingException;
import com.aliyun.tablestore.kafka.connect.utils.TransformException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.fail;

/**
 * 数据类型转换测试
 */
public class DefaultEventParserTest {
    EventParser parser;

    @Before
    public void before() {
        parser = new DefaultEventParser();
    }

    @Test
    public void testStructSchemaForPk() {
        Schema schema = SchemaBuilder.struct()
                .field("string", Schema.STRING_SCHEMA)
                .field("byte", Schema.INT8_SCHEMA)
                .field("short", Schema.INT16_SCHEMA)
                .field("int", Schema.INT32_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("bytes", Schema.BYTES_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("string", "test")
                .put("byte", (byte) 1)
                .put("short", (short) 1)
                .put("int", 1)
                .put("long", 1L)
                .put("float", 1.0f)
                .put("double", 1.0)
                .put("boolean", true)
                .put("bytes", "test".getBytes());

        //string 类型
        PrimaryKey expectedPrimaryKey1 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("string", PrimaryKeyValue.fromString("test"))
                .build();

        List<PrimaryKeySchema> pkDefinedInConfig1 = new ArrayList<>();
        pkDefinedInConfig1.add(new PrimaryKeySchema("string", PrimaryKeyType.STRING));

        PrimaryKey primaryKey1 = parser.parseForPrimaryKey(schema, struct, pkDefinedInConfig1);

        Assert.assertTrue(primaryKey1.compareTo(expectedPrimaryKey1) == 0);

        //integer 类型
        PrimaryKey expectedPrimaryKey2 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("byte", PrimaryKeyValue.fromLong(1))
                .addPrimaryKeyColumn("short", PrimaryKeyValue.fromLong(1))
                .addPrimaryKeyColumn("int", PrimaryKeyValue.fromLong(1))
                .addPrimaryKeyColumn("long", PrimaryKeyValue.fromLong(1))
                .build();

        List<PrimaryKeySchema> pkDefinedInConfig2 = new ArrayList<>();
        pkDefinedInConfig2.add(new PrimaryKeySchema("byte", PrimaryKeyType.INTEGER));
        pkDefinedInConfig2.add(new PrimaryKeySchema("short", PrimaryKeyType.INTEGER));
        pkDefinedInConfig2.add(new PrimaryKeySchema("int", PrimaryKeyType.INTEGER));
        pkDefinedInConfig2.add(new PrimaryKeySchema("long", PrimaryKeyType.INTEGER));

        PrimaryKey primaryKey2 = parser.parseForPrimaryKey(schema, struct, pkDefinedInConfig2);

        Assert.assertTrue(primaryKey2.compareTo(expectedPrimaryKey2) == 0);

        //binary 类型
        PrimaryKey expectedPrimaryKey3 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("bytes", PrimaryKeyValue.fromBinary("test".getBytes()))
                .build();

        List<PrimaryKeySchema> pkDefinedInConfig3 = new ArrayList<>();
        pkDefinedInConfig3.add(new PrimaryKeySchema("bytes", PrimaryKeyType.BINARY));

        PrimaryKey primaryKey3 = parser.parseForPrimaryKey(schema, struct, pkDefinedInConfig3);

        Assert.assertTrue(primaryKey3.compareTo(expectedPrimaryKey3) == 0);
    }

    @Test
    public void testNullSchemaForPk() {
        Schema schema = SchemaBuilder.struct()
                .field("string", Schema.STRING_SCHEMA)
                .field("byte", Schema.INT8_SCHEMA)
                .field("short", Schema.INT16_SCHEMA)
                .field("int", Schema.INT32_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("bytes", Schema.BYTES_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("string", "test")
                .put("byte", (byte) 1)
                .put("short", (short) 1)
                .put("int", 1)
                .put("long", 1L)
                .put("float", 1.0f)
                .put("double", 1.0)
                .put("boolean", true)
                .put("bytes", "test".getBytes());

        //JsonConverter
        Map<String, String> jsonProps = new HashMap<>();
        jsonProps.put(JsonConverterConfig.TYPE_CONFIG, "value");
        jsonProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");

        JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(jsonProps);
        byte[] bytes = jsonConverter.fromConnectData("test", schema, struct);
        SchemaAndValue schemaAndValue = jsonConverter.toConnectData("test", bytes);

        //string 类型转 binary
        PrimaryKey expectedPrimaryKey1 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("string", PrimaryKeyValue.fromBinary("test".getBytes()))
                .build();

        List<PrimaryKeySchema> pkDefinedInConfig1 = new ArrayList<>();
        pkDefinedInConfig1.add(new PrimaryKeySchema("string", PrimaryKeyType.BINARY));

        PrimaryKey primaryKey1 = parser.parseForPrimaryKey(schemaAndValue.schema(), schemaAndValue.value(), pkDefinedInConfig1);

        Assert.assertTrue(primaryKey1.compareTo(expectedPrimaryKey1) == 0);

        //long 类型转 binary
        PrimaryKey expectedPrimaryKey2 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("byte", PrimaryKeyValue.fromBinary(Bytes.toBytes(1L)))
                .addPrimaryKeyColumn("short", PrimaryKeyValue.fromBinary(Bytes.toBytes(1L)))
                .addPrimaryKeyColumn("int", PrimaryKeyValue.fromBinary(Bytes.toBytes(1L)))
                .addPrimaryKeyColumn("long", PrimaryKeyValue.fromBinary(Bytes.toBytes(1L)))
                .build();

        List<PrimaryKeySchema> pkDefinedInConfig2 = new ArrayList<>();
        pkDefinedInConfig2.add(new PrimaryKeySchema("byte", PrimaryKeyType.BINARY));
        pkDefinedInConfig2.add(new PrimaryKeySchema("short", PrimaryKeyType.BINARY));
        pkDefinedInConfig2.add(new PrimaryKeySchema("int", PrimaryKeyType.BINARY));
        pkDefinedInConfig2.add(new PrimaryKeySchema("long", PrimaryKeyType.BINARY));

        PrimaryKey primaryKey2 = parser.parseForPrimaryKey(schemaAndValue.schema(), schemaAndValue.value(), pkDefinedInConfig2);


        Assert.assertTrue(primaryKey2.compareTo(expectedPrimaryKey2) == 0);

        //double 类型转 binary
        PrimaryKey expectedPrimaryKey3 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("float", PrimaryKeyValue.fromBinary(Bytes.toBytes(1.0)))
                .addPrimaryKeyColumn("double", PrimaryKeyValue.fromBinary(Bytes.toBytes(1.0)))
                .build();

        List<PrimaryKeySchema> pkDefinedInConfig3 = new ArrayList<>();
        pkDefinedInConfig3.add(new PrimaryKeySchema("float", PrimaryKeyType.BINARY));
        pkDefinedInConfig3.add(new PrimaryKeySchema("double", PrimaryKeyType.BINARY));

        PrimaryKey primaryKey3 = parser.parseForPrimaryKey(schemaAndValue.schema(), schemaAndValue.value(), pkDefinedInConfig3);

        Assert.assertTrue(primaryKey3.compareTo(expectedPrimaryKey3) == 0);

        //boolean 类型转 binary
        PrimaryKey expectedPrimaryKey4 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("boolean", PrimaryKeyValue.fromBinary(Bytes.toBytes(true)))
                .build();

        List<PrimaryKeySchema> pkDefinedInConfig4 = new ArrayList<>();
        pkDefinedInConfig4.add(new PrimaryKeySchema("boolean", PrimaryKeyType.BINARY));

        PrimaryKey primaryKey4 = parser.parseForPrimaryKey(schemaAndValue.schema(), schemaAndValue.value(), pkDefinedInConfig4);

        Assert.assertTrue(primaryKey4.compareTo(expectedPrimaryKey4) == 0);
    }

    @Test
    public void testMapSchemaForPk() {
        //string 类型转 binary
        Schema map_string = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.STRING_SCHEMA
        ).build();
        Map<String, String> map1 = new HashMap<>();
        map1.put("string", "test");

        PrimaryKey expectedPrimaryKey1 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("string", PrimaryKeyValue.fromBinary("test".getBytes()))
                .build();

        List<PrimaryKeySchema> pkDefinedInConfig1 = new ArrayList<>();
        pkDefinedInConfig1.add(new PrimaryKeySchema("string", PrimaryKeyType.BINARY));

        PrimaryKey primaryKey1 = parser.parseForPrimaryKey(map_string, map1, pkDefinedInConfig1);

        Assert.assertTrue(primaryKey1.compareTo(expectedPrimaryKey1) == 0);

        //int8 类型转 binary
        Schema map_int8 = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.INT8_SCHEMA
        ).build();
        Map<String, Byte> map2 = new HashMap<>();
        map2.put("byte", (byte) 1);

        PrimaryKey expectedPrimaryKey2 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("byte", PrimaryKeyValue.fromBinary((new byte[]{1})))
                .build();

        List<PrimaryKeySchema> pkDefinedInConfig2 = new ArrayList<>();
        pkDefinedInConfig2.add(new PrimaryKeySchema("byte", PrimaryKeyType.BINARY));

        PrimaryKey primaryKey2 = parser.parseForPrimaryKey(map_int8, map2, pkDefinedInConfig2);

        Assert.assertTrue(primaryKey2.compareTo(expectedPrimaryKey2) == 0);

        //int16 类型转 binary
        Schema map_int16 = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.INT16_SCHEMA
        ).build();
        Map<String, Short> map3 = new HashMap<>();
        map3.put("short", (short) 1);

        PrimaryKey expectedPrimaryKey3 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("short", PrimaryKeyValue.fromBinary(Bytes.toBytes((short) 1)))
                .build();

        List<PrimaryKeySchema> pkDefinedInConfig3 = new ArrayList<>();
        pkDefinedInConfig3.add(new PrimaryKeySchema("short", PrimaryKeyType.BINARY));

        PrimaryKey primaryKey3 = parser.parseForPrimaryKey(map_int16, map3, pkDefinedInConfig3);

        Assert.assertTrue(primaryKey3.compareTo(expectedPrimaryKey3) == 0);

        //int32 类型转 binary
        Schema map_int32 = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.INT32_SCHEMA
        ).build();
        Map<String, Integer> map4 = new HashMap<>();
        map4.put("int", 1);

        PrimaryKey expectedPrimaryKey4 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("int", PrimaryKeyValue.fromBinary(Bytes.toBytes(1)))
                .build();

        List<PrimaryKeySchema> pkDefinedInConfig4 = new ArrayList<>();
        pkDefinedInConfig4.add(new PrimaryKeySchema("int", PrimaryKeyType.BINARY));

        PrimaryKey primaryKey4 = parser.parseForPrimaryKey(map_int32, map4, pkDefinedInConfig4);

        Assert.assertTrue(primaryKey4.compareTo(expectedPrimaryKey4) == 0);

        //int64 类型转 binary
        Schema map_int64 = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.INT64_SCHEMA
        ).build();
        Map<String, Long> map5 = new HashMap<>();
        map5.put("long", 1L);

        PrimaryKey expectedPrimaryKey5 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("long", PrimaryKeyValue.fromBinary(Bytes.toBytes(1L)))
                .build();

        List<PrimaryKeySchema> pkDefinedInConfig5 = new ArrayList<>();
        pkDefinedInConfig5.add(new PrimaryKeySchema("long", PrimaryKeyType.BINARY));

        PrimaryKey primaryKey5 = parser.parseForPrimaryKey(map_int64, map5, pkDefinedInConfig5);

        Assert.assertTrue(primaryKey5.compareTo(expectedPrimaryKey5) == 0);

        //float32 类型转 binary
        Schema map_float32 = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.FLOAT32_SCHEMA
        ).build();
        Map<String, Float> map6 = new HashMap<>();
        map6.put("float", 1.0f);

        PrimaryKey expectedPrimaryKey6 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("float", PrimaryKeyValue.fromBinary(Bytes.toBytes(1.0f)))
                .build();

        List<PrimaryKeySchema> pkDefinedInConfig6 = new ArrayList<>();
        pkDefinedInConfig6.add(new PrimaryKeySchema("float", PrimaryKeyType.BINARY));

        PrimaryKey primaryKey6 = parser.parseForPrimaryKey(map_float32, map6, pkDefinedInConfig6);

        Assert.assertTrue(primaryKey6.compareTo(expectedPrimaryKey6) == 0);

        //float64 类型转 binary
        Schema map_float64 = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.INT64_SCHEMA
        ).build();
        Map<String, Double> map7 = new HashMap<>();
        map7.put("double", 1.0);

        PrimaryKey expectedPrimaryKey7 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("double", PrimaryKeyValue.fromBinary(Bytes.toBytes(1.0)))
                .build();

        List<PrimaryKeySchema> pkDefinedInConfig7 = new ArrayList<>();
        pkDefinedInConfig7.add(new PrimaryKeySchema("double", PrimaryKeyType.BINARY));

        PrimaryKey primaryKey7 = parser.parseForPrimaryKey(map_float64, map7, pkDefinedInConfig7);

        Assert.assertTrue(primaryKey7.compareTo(expectedPrimaryKey7) == 0);

        //boolean 类型转 binary
        Schema map_boolean = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA
        ).build();
        Map<String, Boolean> map8 = new HashMap<>();
        map8.put("boolean", true);

        PrimaryKey expectedPrimaryKey8 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("boolean", PrimaryKeyValue.fromBinary(Bytes.toBytes(true)))
                .build();

        List<PrimaryKeySchema> pkDefinedInConfig8 = new ArrayList<>();
        pkDefinedInConfig8.add(new PrimaryKeySchema("boolean", PrimaryKeyType.BINARY));

        PrimaryKey primaryKey8 = parser.parseForPrimaryKey(map_boolean, map8, pkDefinedInConfig8);

        Assert.assertTrue(primaryKey8.compareTo(expectedPrimaryKey8) == 0);

    }

    @Test
    public void testStructSchemaForColumn() {
        Schema schema = SchemaBuilder.struct()
                .field("string", Schema.STRING_SCHEMA)
                .field("byte", Schema.INT8_SCHEMA)
                .field("short", Schema.INT16_SCHEMA)
                .field("int", Schema.INT32_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("bytes", Schema.BYTES_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("string", "test")
                .put("byte", (byte) 1)
                .put("short", (short) 1)
                .put("int", 1)
                .put("long", 1L)
                .put("float", 1.0f)
                .put("double", 1.0)
                .put("boolean", true)
                .put("bytes", "test".getBytes());


        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("topic_partition", PrimaryKeyValue.fromString("test_0"))
                .addPrimaryKeyColumn("offset", PrimaryKeyValue.fromLong(0))
                .build();

        LinkedHashMap<String, ColumnValue> columnsMap = parser.parseForColumns(schema, struct, primaryKey, new ArrayList<DefinedColumnSchema>());

        Assert.assertEquals("test", columnsMap.get("string").asString());
        Assert.assertEquals((byte) 1, columnsMap.get("byte").asLong());
        Assert.assertEquals((short) 1, columnsMap.get("short").asLong());
        Assert.assertEquals(1, columnsMap.get("int").asLong());
        Assert.assertEquals(1L, columnsMap.get("long").asLong());
        Assert.assertTrue(1.0f == columnsMap.get("float").asDouble());
        Assert.assertTrue(1.0 == columnsMap.get("double").asDouble());
        Assert.assertEquals(true, columnsMap.get("boolean").asBoolean());
        Assert.assertEquals("test", new String(columnsMap.get("bytes").asBinary()));
    }

    @Test
    public void testNullSchemaForColumn() {
        Schema schema = SchemaBuilder.struct()
                .field("string", Schema.STRING_SCHEMA)
                .field("byte", Schema.INT8_SCHEMA)
                .field("short", Schema.INT16_SCHEMA)
                .field("int", Schema.INT32_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("bytes", Schema.BYTES_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("string", "test")
                .put("byte", (byte) 1)
                .put("short", (short) 1)
                .put("int", 1)
                .put("long", 1L)
                .put("float", 1.0f)
                .put("double", 1.0)
                .put("boolean", true)
                .put("bytes", "test".getBytes());

        //JsonConverter
        Map<String, String> jsonProps = new HashMap<>();
        jsonProps.put(JsonConverterConfig.TYPE_CONFIG, "value");
        jsonProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");

        JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(jsonProps);
        byte[] bytes = jsonConverter.fromConnectData("test", schema, struct);
        SchemaAndValue schemaAndValue = jsonConverter.toConnectData("test", bytes);

        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("topic_partition", PrimaryKeyValue.fromString("test_0"))
                .addPrimaryKeyColumn("offset", PrimaryKeyValue.fromLong(0))
                .build();

        LinkedHashMap<String, ColumnValue> columnsMap = parser.parseForColumns(schemaAndValue.schema(), schemaAndValue.value(), primaryKey, new ArrayList<DefinedColumnSchema>());

        Assert.assertEquals("test", Bytes.toString(columnsMap.get("string").asBinary()));
        Assert.assertEquals(1L, Bytes.toLong(columnsMap.get("byte").asBinary()));
        Assert.assertEquals(1L, Bytes.toLong(columnsMap.get("short").asBinary()));
        Assert.assertEquals(1L, Bytes.toLong(columnsMap.get("int").asBinary()));
        Assert.assertEquals(1L, Bytes.toLong(columnsMap.get("long").asBinary()));
        Assert.assertTrue(1.0 == Bytes.toDouble(columnsMap.get("float").asBinary()));
        Assert.assertTrue(1.0 == Bytes.toDouble(columnsMap.get("double").asBinary()));
        Assert.assertEquals(true, Bytes.toBoolean(columnsMap.get("boolean").asBinary()));
    }

    @Test
    public void testMapSchemaForColumn() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("topic_partition", PrimaryKeyValue.fromString("test_0"))
                .addPrimaryKeyColumn("offset", PrimaryKeyValue.fromLong(0))
                .build();

        //string 类型转 binary
        Schema map_string = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.STRING_SCHEMA
        ).build();
        Map<String, String> map1 = new HashMap<>();
        map1.put("string", "test");

        LinkedHashMap<String, ColumnValue> columnsMap1 = parser.parseForColumns(map_string, map1, primaryKey, new ArrayList<DefinedColumnSchema>());

        Assert.assertEquals("test", Bytes.toString(columnsMap1.get("string").asBinary()));

        //int8 类型转 binary
        Schema map_int8 = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.INT8_SCHEMA
        ).build();
        Map<String, Byte> map2 = new HashMap<>();
        map2.put("byte", (byte) 1);

        LinkedHashMap<String, ColumnValue> columnsMap2 = parser.parseForColumns(map_int8, map2, primaryKey, new ArrayList<DefinedColumnSchema>());

        Assert.assertEquals((byte) 1, columnsMap2.get("byte").asBinary()[0]);

        //int16 类型转 binary
        Schema map_int16 = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.INT16_SCHEMA
        ).build();
        Map<String, Short> map3 = new HashMap<>();
        map3.put("short", (short) 1);

        LinkedHashMap<String, ColumnValue> columnsMap3 = parser.parseForColumns(map_int16, map3, primaryKey, new ArrayList<DefinedColumnSchema>());

        Assert.assertEquals((short) 1, Bytes.toShort(columnsMap3.get("short").asBinary()));

        //int32 类型转 binary
        Schema map_int32 = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.INT32_SCHEMA
        ).build();
        Map<String, Integer> map4 = new HashMap<>();
        map4.put("int", 1);

        LinkedHashMap<String, ColumnValue> columnsMap4 = parser.parseForColumns(map_int32, map4, primaryKey, new ArrayList<DefinedColumnSchema>());

        Assert.assertEquals(1, Bytes.toInt(columnsMap4.get("int").asBinary()));

        //int64 类型转 binary
        Schema map_int64 = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.INT64_SCHEMA
        ).build();
        Map<String, Long> map5 = new HashMap<>();
        map5.put("long", 1L);

        LinkedHashMap<String, ColumnValue> columnsMap5 = parser.parseForColumns(map_int64, map5, primaryKey, new ArrayList<DefinedColumnSchema>());

        Assert.assertEquals(1L, Bytes.toLong(columnsMap5.get("long").asBinary()));

        //float32 类型转 binary
        Schema map_float32 = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.FLOAT32_SCHEMA
        ).build();
        Map<String, Float> map6 = new HashMap<>();
        map6.put("float", 1.0f);

        LinkedHashMap<String, ColumnValue> columnsMap6 = parser.parseForColumns(map_float32, map6, primaryKey, new ArrayList<DefinedColumnSchema>());

        Assert.assertTrue(1.0f == Bytes.toFloat(columnsMap6.get("float").asBinary()));

        //float64 类型转 binary
        Schema map_float64 = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.INT64_SCHEMA
        ).build();
        Map<String, Double> map7 = new HashMap<>();
        map7.put("double", 1.0);

        LinkedHashMap<String, ColumnValue> columnsMap7 = parser.parseForColumns(map_float64, map7, primaryKey, new ArrayList<DefinedColumnSchema>());

        Assert.assertTrue(1.0 == Bytes.toDouble(columnsMap7.get("double").asBinary()));

        //boolean 类型转 binary
        Schema map_boolean = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA
        ).build();
        Map<String, Boolean> map8 = new HashMap<>();
        map8.put("boolean", true);

        LinkedHashMap<String, ColumnValue> columnsMap8 = parser.parseForColumns(map_boolean, map8, primaryKey, new ArrayList<DefinedColumnSchema>());

        Assert.assertEquals(true, Bytes.toBoolean(columnsMap8.get("boolean").asBinary()));

        //binary
        Schema map_bytes = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.BYTES_SCHEMA
        ).build();
        Map<String, byte[]> map9 = new HashMap<>();
        map9.put("bytes", "test".getBytes());

        LinkedHashMap<String, ColumnValue> columnsMap9 = parser.parseForColumns(map_bytes, map9, primaryKey, new ArrayList<DefinedColumnSchema>());

        Assert.assertEquals("test", new String(columnsMap9.get("bytes").asBinary()));
    }

    @Test
    public void testAutoIncrementSchemaForPk() {
        Schema schema = SchemaBuilder.struct()
                .field("string", Schema.STRING_SCHEMA)
                .field("byte", Schema.INT8_SCHEMA)
                .field("short", Schema.INT16_SCHEMA)
                .field("int", Schema.INT32_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("bytes", Schema.BYTES_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("string", "test")
                .put("byte", (byte) 1)
                .put("short", (short) 1)
                .put("int", 1)
                .put("long", 1L)
                .put("float", 1.0f)
                .put("double", 1.0)
                .put("boolean", true)
                .put("bytes", "test".getBytes());


        List<PrimaryKeySchema> pkDefinedInConfig = new ArrayList<>();
        pkDefinedInConfig.add(new PrimaryKeySchema("auto_increment", PrimaryKeyType.INTEGER,PrimaryKeyOption.AUTO_INCREMENT));

        PrimaryKey primaryKey = parser.parseForPrimaryKey(schema, struct, pkDefinedInConfig);

        Assert.assertEquals(PrimaryKeyValue.AUTO_INCREMENT,primaryKey.getPrimaryKeyColumn("auto_increment").getValue());
        Assert.assertEquals(PrimaryKeyValue.AUTO_INCREMENT.hashCode(),primaryKey.getPrimaryKeyColumn("auto_increment").getValue().hashCode());
    }

    @Test
    public void testErrorType(){
        List<PrimaryKeySchema> pkDefinedInConfig = new ArrayList<>();
        pkDefinedInConfig.add(new PrimaryKeySchema("string", PrimaryKeyType.STRING));
        try{
            PrimaryKey primaryKey = parser.parseForPrimaryKey(Schema.STRING_SCHEMA,"test", pkDefinedInConfig);
            fail();
        }catch (EventParsingException e){

        }
    }

    @Test
    public void testErrorType2(){
        Schema interSchema = SchemaBuilder.struct()
                .field("string", Schema.STRING_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("struct", interSchema)
                .build();

        Struct interStruct = new Struct(interSchema)
                .put("string", "test");

        Struct struct = new Struct(schema)
                .put("struct", interStruct);

        List<PrimaryKeySchema> pkDefinedInConfig = new ArrayList<>();
        pkDefinedInConfig.add(new PrimaryKeySchema("string", PrimaryKeyType.STRING));
        try{
            PrimaryKey primaryKey = parser.parseForPrimaryKey(schema,struct, pkDefinedInConfig);
            fail();
        }catch (EventParsingException e){

        }

        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("topic_partition", PrimaryKeyValue.fromString("test_0"))
                .addPrimaryKeyColumn("offset", PrimaryKeyValue.fromLong(0))
                .build();

        try{
            LinkedHashMap<String, ColumnValue> columnsMap = parser.parseForColumns(schema, struct, primaryKey, new ArrayList<DefinedColumnSchema>());
            fail();
        }catch (EventParsingException e){

        }
      }

    @Test
    public void testErrorType3(){
        Schema interSchema = SchemaBuilder.struct()
                .field("string", Schema.STRING_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.map(
                Schema.STRING_SCHEMA,interSchema
        );

        Struct interStruct = new Struct(interSchema)
                .put("string", "test");

        Map<String,Struct> map=new HashMap<>();
        map.put("struct",interStruct);

        List<PrimaryKeySchema> pkDefinedInConfig = new ArrayList<>();
        pkDefinedInConfig.add(new PrimaryKeySchema("string", PrimaryKeyType.STRING));
        try{
            PrimaryKey primaryKey = parser.parseForPrimaryKey(schema,map, pkDefinedInConfig);
            fail();
        }catch (EventParsingException e){

        }

        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("topic_partition", PrimaryKeyValue.fromString("test_0"))
                .addPrimaryKeyColumn("offset", PrimaryKeyValue.fromLong(0))
                .build();

        try{
            LinkedHashMap<String, ColumnValue> columnsMap = parser.parseForColumns(schema, map, primaryKey, new ArrayList<DefinedColumnSchema>());
            fail();
        }catch (EventParsingException e){

        }
    }



}