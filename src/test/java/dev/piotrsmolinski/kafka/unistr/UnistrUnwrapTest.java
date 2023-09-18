package dev.piotrsmolinski.kafka.unistr;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.TransformationChain;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class UnistrUnwrapTest {

  UnistrUnwrap<SourceRecord> smt = new UnistrUnwrap<>();

  @Test
  void testTextUnwrap() {

    Assertions.assertThat(smt.unwrap("UNISTR('Not Suspicious erkl\\00E4rt test \\00E4 \\00F6 \\00FC test')"))
            .isEqualTo("Not Suspicious erklärt test ä ö ü test");

  }

  @Test
  void testRecordUnwrap() {

    Schema valueSchema = SchemaBuilder.struct()
            .field("text1", SchemaBuilder.string().build())
            .field("text2", SchemaBuilder.string().optional().build())
            .field("int1", SchemaBuilder.int32().build())
            .build();

    SourceRecord inputRecord = new SourceRecord(
            new HashMap<>(),
            new HashMap<>(),
            "test-topic",
            5,
            SchemaBuilder.string().optional().build(),
            "key",
            valueSchema,
            new Struct(valueSchema)
                    .put("text1", "0123456789")
                    .put("text2", "UNISTR('Not Suspicious erkl\\00E4rt test \\00E4 \\00F6 \\00FC test')")
                    .put("int1", 29),
            System.currentTimeMillis(),
            new ConnectHeaders()
    );

    SourceRecord outputRecord = smt.apply(inputRecord);

    Assertions.assertThat(outputRecord.sourcePartition())
            .isSameAs(inputRecord.sourcePartition());
    Assertions.assertThat(outputRecord.sourceOffset())
            .isSameAs(inputRecord.sourceOffset());
    Assertions.assertThat(outputRecord.topic())
            .isEqualTo(inputRecord.topic());
    Assertions.assertThat(outputRecord.kafkaPartition())
            .isEqualTo(inputRecord.kafkaPartition());

    Assertions.assertThat(outputRecord.keySchema())
            .isEqualTo(inputRecord.keySchema());
    Assertions.assertThat(outputRecord.key())
            .isEqualTo(inputRecord.key());
    Assertions.assertThat(outputRecord.valueSchema())
            .isEqualTo(inputRecord.valueSchema());

    Assertions.assertThat(outputRecord.value())
            .isInstanceOf(Struct.class);
    Assertions.assertThat(outputRecord.value())
            .asInstanceOf(InstanceOfAssertFactories.type(Struct.class))
            .extracting(s->s.get("text1"))
            .isEqualTo("0123456789");
    Assertions.assertThat(outputRecord.value())
            .asInstanceOf(InstanceOfAssertFactories.type(Struct.class))
            .extracting(s->s.get("text2"))
            .isEqualTo("Not Suspicious erklärt test ä ö ü test");
    Assertions.assertThat(outputRecord.value())
            .asInstanceOf(InstanceOfAssertFactories.type(Struct.class))
            .extracting(s->s.get("int1"))
            .isEqualTo(29);

    Assertions.assertThat(outputRecord.timestamp())
            .isEqualTo(inputRecord.timestamp());
    Assertions.assertThat(outputRecord.headers())
            .isSameAs(inputRecord.headers());

  }

  @Test
  void testTransformationChain() {

    ConnectorConfig config = new ConnectorConfig(new Plugins(Collections.emptyMap()), Map.ofEntries(
            Map.entry("name", "dummy"),
            Map.entry("connector.class", "dev.piotrsmolinski.kafka.unistr.DummySourceConnector"),
            Map.entry("max.tasks", "1"),
            Map.entry("transforms", "unistr.unwrap"),
            Map.entry("transforms.unistr.unwrap.type", "dev.piotrsmolinski.kafka.unistr.UnistrUnwrap")
    ));

    RetryWithToleranceOperator retry = new RetryWithToleranceOperator(
    0l, 0l, ToleranceType.NONE, Time.SYSTEM, Mockito.mock(ErrorHandlingMetrics.class)
    );

    TransformationChain<SourceRecord> chain = new TransformationChain<>(
            config.<SourceRecord>transformationStages(),
            retry
    );

    Schema valueSchema = SchemaBuilder.struct()
            .field("text1", SchemaBuilder.string().build())
            .field("text2", SchemaBuilder.string().optional().build())
            .field("int1", SchemaBuilder.int32().build())
            .build();

    SourceRecord inputRecord = new SourceRecord(
            new HashMap<>(),
            new HashMap<>(),
            "test-topic",
            5,
            SchemaBuilder.string().optional().build(),
            "key",
            valueSchema,
            new Struct(valueSchema)
                    .put("text1", "0123456789")
                    .put("text2", "UNISTR('Not Suspicious erkl\\00E4rt test \\00E4 \\00F6 \\00FC test')")
                    .put("int1", 29),
            System.currentTimeMillis(),
            new ConnectHeaders()
    );

    SourceRecord outputRecord = chain.apply(inputRecord);

    Assertions.assertThat(outputRecord.sourcePartition())
            .isSameAs(inputRecord.sourcePartition());
    Assertions.assertThat(outputRecord.sourceOffset())
            .isSameAs(inputRecord.sourceOffset());
    Assertions.assertThat(outputRecord.topic())
            .isEqualTo(inputRecord.topic());
    Assertions.assertThat(outputRecord.kafkaPartition())
            .isEqualTo(inputRecord.kafkaPartition());

    Assertions.assertThat(outputRecord.keySchema())
            .isEqualTo(inputRecord.keySchema());
    Assertions.assertThat(outputRecord.key())
            .isEqualTo(inputRecord.key());
    Assertions.assertThat(outputRecord.valueSchema())
            .isEqualTo(inputRecord.valueSchema());

    Assertions.assertThat(outputRecord.value())
            .isInstanceOf(Struct.class);
    Assertions.assertThat(outputRecord.value())
            .asInstanceOf(InstanceOfAssertFactories.type(Struct.class))
            .extracting(s->s.get("text1"))
            .isEqualTo("0123456789");
    Assertions.assertThat(outputRecord.value())
            .asInstanceOf(InstanceOfAssertFactories.type(Struct.class))
            .extracting(s->s.get("text2"))
            .isEqualTo("Not Suspicious erklärt test ä ö ü test");
    Assertions.assertThat(outputRecord.value())
            .asInstanceOf(InstanceOfAssertFactories.type(Struct.class))
            .extracting(s->s.get("int1"))
            .isEqualTo(29);

    Assertions.assertThat(outputRecord.timestamp())
            .isEqualTo(inputRecord.timestamp());
    Assertions.assertThat(outputRecord.headers())
            .isSameAs(inputRecord.headers());

  }

  @Test
  void testSkipRedoLog() {

    ConnectorConfig config = new ConnectorConfig(new Plugins(Collections.emptyMap()), Map.ofEntries(
            Map.entry("name", "dummy"),
            Map.entry("connector.class", "dev.piotrsmolinski.kafka.unistr.DummySourceConnector"),
            Map.entry("max.tasks", "1"),
            Map.entry("transforms", "unistr.unwrap"),
            Map.entry("transforms.unistr.unwrap.type", "dev.piotrsmolinski.kafka.unistr.UnistrUnwrap"),
            Map.entry("transforms.unistr.unwrap.predicate", "is.redo.log"),
            Map.entry("transforms.unistr.unwrap.negate", "true"),
            Map.entry("predicates", "is.redo.log"),
            Map.entry("predicates.is.redo.log.type", "org.apache.kafka.connect.transforms.predicates.TopicNameMatches"),
            Map.entry("predicates.is.redo.log.pattern", "test-redo-log")
    ));

    RetryWithToleranceOperator retry = new RetryWithToleranceOperator(
    0l, 0l, ToleranceType.NONE, Time.SYSTEM, Mockito.mock(ErrorHandlingMetrics.class)
    );

    TransformationChain<SourceRecord> chain = new TransformationChain<>(
            config.<SourceRecord>transformationStages(),
            retry
    );

    Schema valueSchema = SchemaBuilder.struct()
            .field("text", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    SourceRecord rawInputRecord = new SourceRecord(
            new HashMap<>(),
            new HashMap<>(),
            "test-topic",
            5,
            SchemaBuilder.string().optional().build(),
            "key",
            valueSchema,
            new Struct(valueSchema)
                    .put("text", "UNISTR('Not Suspicious erkl\\00E4rt test \\00E4 \\00F6 \\00FC test')"),
            System.currentTimeMillis(),
            new ConnectHeaders()
    );

    SourceRecord rawOutputRecord = chain.apply(rawInputRecord);

    Assertions.assertThat(rawOutputRecord.value())
            .asInstanceOf(InstanceOfAssertFactories.type(Struct.class))
            .extracting(s->s.get("text"))
            .isEqualTo("Not Suspicious erklärt test ä ö ü test");

    SourceRecord redoLogInputRecord = new SourceRecord(
            new HashMap<>(),
            new HashMap<>(),
            "test-redo-log",
            5,
            SchemaBuilder.string().optional().build(),
            "key",
            valueSchema,
            new Struct(valueSchema)
                    .put("text", "UNISTR('Not Suspicious erkl\\00E4rt test \\00E4 \\00F6 \\00FC test')"),
            System.currentTimeMillis(),
            new ConnectHeaders()
    );

    SourceRecord redoLogOutputRecord = chain.apply(redoLogInputRecord);

    Assertions.assertThat(redoLogOutputRecord.value())
            .asInstanceOf(InstanceOfAssertFactories.type(Struct.class))
            .extracting(s->s.get("text"))
            .isNotEqualTo("Not Suspicious erklärt test ä ö ü test");

  }

}
