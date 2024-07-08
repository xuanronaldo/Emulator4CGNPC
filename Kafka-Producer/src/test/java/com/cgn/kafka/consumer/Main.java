package com.cgn.kafka.consumer;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import com.cgn.kafka.producer.KafkaRecord;
import com.google.gson.*;

import java.lang.reflect.Type;
import java.util.Arrays;

public class Main {
  //    private static Gson gson = new GsonBuilder()
  //            .registerTypeAdapter(Object.class, new JsonDeserializer<Object>() {
  //                @Override
  //                public Object deserialize(JsonElement json, Type typeOfT,
  // JsonDeserializationContext context) throws JsonParseException {
  //                    if (json.isJsonPrimitive()) {
  //                        JsonPrimitive primitive = json.getAsJsonPrimitive();
  //                        if (primitive.isNumber()) {
  //                            double value = primitive.getAsDouble();
  //                            if (value == (int) value) {
  //                                return (int) value;
  //                            } else {
  //                                return value;
  //                            }
  //                        } else if (primitive.isBoolean()) {
  //                            return primitive.getAsBoolean();
  //                        } else if (primitive.isString()) {
  //                            return primitive.getAsString();
  //                        }
  //                    } else if (json.isJsonArray()) {
  //                        return context.deserialize(json, Object[].class);
  //                    } else if (json.isJsonObject()) {
  //                        return context.deserialize(json, Object.class);
  //                    }
  //                    return null;
  //                }
  //            })
  //            .create();

  public static void main(String[] args) {
    // 示例数据
    KafkaRecord kafkaRecord =
        new KafkaRecord(
            "root",
            Arrays.asList("s1"),
            213213L,
            Arrays.asList(TSDataType.INT32),
            Arrays.asList(1));
    //        MyClass myObject = new MyClass(20);

    Gson gson =
        new GsonBuilder()
            .registerTypeAdapter(
                Object.class,
                new JsonDeserializer<Object>() {
                  @Override
                  public Object deserialize(
                      JsonElement json, Type typeOfT, JsonDeserializationContext context)
                      throws JsonParseException {
                    if (json.isJsonPrimitive()) {
                      JsonPrimitive primitive = json.getAsJsonPrimitive();
                      if (primitive.isNumber()) {
                        double value = primitive.getAsDouble();
                        if (value == (int) value) {
                          return (int) value;
                        } else {
                          return value;
                        }
                      } else if (primitive.isBoolean()) {
                        return primitive.getAsBoolean();
                      } else if (primitive.isString()) {
                        return primitive.getAsString();
                      }
                    } else if (json.isJsonArray()) {
                      return context.deserialize(json, Object[].class);
                    } else if (json.isJsonObject()) {
                      return context.deserialize(json, Object.class);
                    }
                    return null;
                  }
                })
            .create();
    // 序列化对象
    String json = gson.toJson(kafkaRecord);
    System.out.println(json); // {"value":20}

    // 反序列化对象
    KafkaRecord deserializedObject = gson.fromJson(json, KafkaRecord.class);
    System.out.println(deserializedObject); // MyClass{value=20}
  }
}

class MyClass {
  private Number value;

  public MyClass(Number value) {
    this.value = value;
  }

  public Number getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "MyClass{" + "value=" + value + '}';
  }
}
