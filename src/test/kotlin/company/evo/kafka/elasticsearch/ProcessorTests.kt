package company.evo.kafka.elasticsearch

import com.google.protobuf.Int32Value
import com.google.protobuf.Struct
import com.google.protobuf.Timestamp
import com.google.protobuf.Value

import company.evo.kafka.TestProto
import company.evo.bulk.elasticsearch.BulkAction

import io.kotlintest.matchers.string.contain
import io.kotlintest.should
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec

class JsonProcessorTests : StringSpec() {
    private val processor = JsonProcessor()

    init {
        "empty map" {
            shouldThrow<IllegalArgumentException> {
                processor.process(mapOf<Any, Any?>(), "test_index")
            }
        }

        "invalid payload" {
            shouldThrow<IllegalArgumentException> {
                processor.process(mapOf("payload" to "<invalid>"), "test_index")
            }.also { exc ->
                exc.message should contain("[payload]")
            }
        }

        "missing action" {
            shouldThrow<IllegalArgumentException> {
                processor.process(mapOf("source" to mapOf<Any,Any?>()), "test_index")
            }.also { exc ->
                exc.message should contain("[action]")
            }
        }

        "missing source" {
            shouldThrow<IllegalArgumentException> {
                processor.process(
                        mapOf(
                                "action" to mapOf(
                                        "index" to mapOf<Any,Any?>()
                                )
                        ),
                        "test_index"
                )
            }.also { exc ->
                exc.message should contain("[source]")
            }
        }

        "unknown action" {
            shouldThrow<IllegalArgumentException> {
                processor.process(
                        mapOf(
                                "action" to mapOf(
                                        "<unknown>" to mapOf<Any,Any?>()
                                )
                        ),
                        "test_index"
                )
            }.also { exc ->
                exc.message should contain("[index, create, update, delete]")
            }
        }

        "process index action" {
            val action = processor.process(
                    mapOf<Any, Any?>(
                            "action" to mapOf(
                                    "index" to mapOf(
                                            "id" to 123,
                                            "type" to "test")
                            ),
                            "source" to mapOf(
                                    "name" to "Teo"
                            )
                    ),
                    "test_index"
            )
            action.operation shouldBe BulkAction.Operation.INDEX
            action.index shouldBe "test_index"
            action.type shouldBe "test"
            action.id shouldBe "123"
            action.routing shouldBe null
            action.parent shouldBe null
            action.version shouldBe null
            action.versionType shouldBe null
            action.source shouldBe mapOf("name" to "Teo")
        }

        "process delete action" {
            val action = processor.process(
                    mapOf<Any, Any?>(
                            "action" to mapOf(
                                    "delete" to mapOf(
                                            "id" to 123,
                                            "type" to "test"
                                    )
                            )
                    ),
                    "test_index"
            )
            action.operation shouldBe BulkAction.Operation.DELETE
            action.index shouldBe "test_index"
            action.type shouldBe "test"
            action.id shouldBe "123"
            action.source shouldBe null
        }

        "process update action" {
            val action = processor.process(
                    mapOf<Any, Any?>(
                            "action" to mapOf(
                                    "update" to mapOf(
                                            "id" to 123,
                                            "type" to "test"
                                    )
                            ),
                            "source" to mapOf(
                                    "doc" to mapOf("name" to "Updated name")
                            )
                    ),
                    "test_index"
            )
            action.operation shouldBe BulkAction.Operation.UPDATE
            action.index shouldBe "test_index"
            action.type shouldBe "test"
            action.id shouldBe "123"
            action.source shouldBe mapOf("doc" to mapOf("name" to "Updated name"))
        }

        "process create action" {
            val action = processor.process(
                    mapOf<Any, Any?>(
                            "action" to mapOf(
                                    "create" to mapOf(
                                            "id" to 123,
                                            "type" to "test")
                            ),
                            "source" to mapOf(
                                    "name" to "Teo"
                            )),
                    "test_index"
            )
            action.operation shouldBe BulkAction.Operation.CREATE
            action.index shouldBe "test_index"
            action.type shouldBe "test"
            action.id shouldBe "123"
            action.source shouldBe mapOf("name" to "Teo")
        }
    }
}

class ProtobufProcessorTests : StringSpec() {
    private val processor = ProtobufProcessor(includeDefaultValues = true)
    private val processorNoDefaults = ProtobufProcessor(includeDefaultValues = false)
    private val testMsg = TestProto.TestMessage.newBuilder()
            .setAction(BulkActionProto.BulkAction.newBuilder()
                    .setOpType(BulkActionProto.BulkAction.OpType.INDEX)
                    .setType("test")
                    .setId("123")
                    .setRouting("4"))
            .setSource(TestProto.TestDocument.newBuilder()
                    .setId(0)
                    .setName("Teo")
                    .build())
            .build()

    init {
        "missing action" {
            shouldThrow<IllegalArgumentException> {
                processor.process(
                        TestProto.MissingActionMessage.getDefaultInstance(),
                        "test_index"

                )
            }.also {
                it.message should contain("[action]")
            }
        }

        "invalid action" {
            shouldThrow<IllegalArgumentException> {
                processor.process(
                        TestProto.InvalidActionMessage.getDefaultInstance(),
                        "test_index"
                )
            }.also {
                it.message should contain("[action]")
                it.message should contain("BulkAction")
            }
        }

        "missing source" {
            shouldThrow<IllegalArgumentException> {
                processor.process(
                        TestProto.MissingSourceMessage.getDefaultInstance(),
                        "test_index"
                )
            }.also {
                it.message should contain("[source]")
            }
        }

        "invalid source" {
            shouldThrow<IllegalArgumentException> {
                processor.process(
                        TestProto.InvalidSourceMessage.getDefaultInstance(),
                        "test_index"
                )
            }.also {
                it.message should contain("[source]")
                it.message should contain("Message")
            }
        }

        "process index action" {
            processorNoDefaults.process(
                    testMsg, "test_index"
            ).run {
                operation shouldBe BulkAction.Operation.INDEX
                index shouldBe "test_index"
                type shouldBe "test"
                id shouldBe "123"
                routing shouldBe "4"
                parent shouldBe null
                source shouldBe """{"name":"Teo"}"""
            }
        }

        "process index action including default values" {
            processor.process(testMsg, "test_index").run {
                operation shouldBe BulkAction.Operation.INDEX
                index shouldBe "test_index"
                type shouldBe "test"
                id shouldBe "123"
                routing shouldBe "4"
                parent shouldBe null
                source shouldBe """{"id":0,"name":"Teo","counter":"0"}"""
            }
        }

        "process delete action" {
            processorNoDefaults.process(
                    BulkActionProto.DeleteMessage.newBuilder()
                            .setAction(BulkActionProto.BulkAction.newBuilder()
                                    .setOpType(BulkActionProto.BulkAction.OpType.DELETE)
                                    .setType("test").setId("123").setRouting("4")
                            ),
                    "test_index"
            ).run {
                operation shouldBe  BulkAction.Operation.DELETE
                index shouldBe "test_index"
                type shouldBe "test"
                id shouldBe "123"
                routing shouldBe "4"
                parent shouldBe null
                source shouldBe null
            }
        }

        "process update action with document" {
            processorNoDefaults.process(
                    TestProto.UpdateMessage.newBuilder()
                            .setAction(BulkActionProto.BulkAction.newBuilder()
                                    .setOpType(BulkActionProto.BulkAction.OpType.UPDATE)
                                    .setType("test").setId("123").setRouting("4")
                            )
                            .setSource(TestProto.UpdateMessage.Source.newBuilder()
                                    .setDoc(TestProto.TestDocument.newBuilder()
                                            .setName("Leo"))
                                    .setDocAsUpsert(true)),
                    "test_index"
            ).run {
                operation shouldBe BulkAction.Operation.UPDATE
                index shouldBe "test_index"
                type shouldBe "test"
                id shouldBe "123"
                routing shouldBe "4"
                parent shouldBe null
                source shouldBe """{"doc":{"name":"Leo"},"doc_as_upsert":true}"""
            }
        }

        "process update action with script" {
            processorNoDefaults.process(
                    TestProto.UpdateMessage.newBuilder()
                            .setAction(BulkActionProto.BulkAction.newBuilder()
                                    .setOpType(BulkActionProto.BulkAction.OpType.UPDATE)
                                    .setType("test").setId("123").setRouting("4")
                            )
                            .setSource(TestProto.UpdateMessage.Source.newBuilder()
                                    .setScript(BulkActionProto.Script.newBuilder()
                                            .setLang("painless")
                                            .setSource("ctx._source.counter += params.count")
                                            .setParams(Struct.newBuilder()
                                                    .putFields("count", Value.newBuilder().setNumberValue(4.0).build())
                                            )
                                    )
                                    .setUpsert(TestProto.TestDocument.newBuilder()
                                            .setName("Teo")
                                            .setCounter(1))
                            ),
                    "test_index"
            ).run {
                operation shouldBe BulkAction.Operation.UPDATE
                index shouldBe "test_index"
                type shouldBe "test"
                id shouldBe "123"
                routing shouldBe "4"
                parent shouldBe null
                source shouldBe """{"script":{""" +
                        """"lang":"painless",""" +
                        """"source":"ctx._source.counter += params.count",""" +
                        """"params":{"count":4.0}},""" +
                        """"upsert":{"name":"Teo","counter":"1"}}"""
            }
        }

        "process create action" {
            processorNoDefaults.process(
                    TestProto.TestMessage.newBuilder()
                            .setAction(BulkActionProto.BulkAction.newBuilder()
                                    .setOpType(BulkActionProto.BulkAction.OpType.CREATE)
                                    .setType("test").setId("123").setRouting("4")
                            )
                            .setSource(TestProto.TestDocument.newBuilder()
                                    .setName("Leo")),
                    "test_index"
            ).run {
                operation shouldBe BulkAction.Operation.CREATE
                index shouldBe "test_index"
                type shouldBe "test"
                id shouldBe "123"
                routing shouldBe "4"
                parent shouldBe null
                source shouldBe """{"name":"Leo"}"""
            }
        }

        "process source with enum" {
            processorNoDefaults.process(
                    TestProto.EnumMessage.newBuilder()
                            .setSource(TestProto.EnumMessage.Source.newBuilder()
                                    .setStatus(TestProto.EnumMessage.Status.DELETED)),
                    "test_index"
            ).run {
                source shouldBe """{"status":"DELETED"}"""
            }
        }

        "source without default enum value" {
            processorNoDefaults.process(
                    TestProto.EnumMessage.newBuilder()
                            .setSource(TestProto.EnumMessage.Source.getDefaultInstance()),
                    "test_index"
            ).run {
                source shouldBe "{}"
            }
        }

        "source including default enum value" {
            processor.process(
                    TestProto.EnumMessage.newBuilder()
                            .setSource(TestProto.EnumMessage.Source.getDefaultInstance()),
                    "test_index"
            ).run {
                source shouldBe """{"status":"ACTIVE"}"""
            }
        }

        "repeated field" {
            processorNoDefaults.process(
                    TestProto.RepeatedMessage.newBuilder()
                            .setSource(TestProto.RepeatedMessage.Source.newBuilder()
                                    .addAllDeliveryRegions(listOf(1, 5))),
                    "test_index"
            ).run {
                source shouldBe """{"delivery_regions":[1,5]}"""
            }
        }

        "empty repeated field" {
            processorNoDefaults.process(
                    TestProto.RepeatedMessage.newBuilder()
                            .setSource(TestProto.RepeatedMessage.Source.getDefaultInstance()),
                    "test_index"
            ).run {
                source shouldBe "{}"
            }
        }

        "source with a map" {
            processorNoDefaults.process(
                    TestProto.MapMessage.newBuilder()
                            .setSource(TestProto.MapMessage.Source.newBuilder()
                                    .putMyMap("test", "test value")),
                    "test_index"
            ).run {
                source shouldBe """{"my_map":{"test":"test value"}}"""
            }
        }

        "source with an empty map" {
            processorNoDefaults.process(
                    TestProto.MapMessage.newBuilder()
                            .setSource(TestProto.MapMessage.Source.getDefaultInstance()),
                    "test_index"
            ).run {
                source shouldBe "{}"
            }
        }

        "source with an empty map including default value" {
            processor.process(
                    TestProto.MapMessage.newBuilder()
                            .setSource(TestProto.MapMessage.Source.getDefaultInstance()),
                    "test_index"
            ).run {
                source shouldBe """{"my_map":{}}"""
            }
        }

        "source with timestamp" {
            processorNoDefaults.process(
                    TestProto.DatetimeMessage.newBuilder()
                            .setSource(TestProto.DatetimeMessage.Source.newBuilder()
                                    .setDatetime(Timestamp.newBuilder().setSeconds(1))),
                    "test_index"
            ).run {
                source shouldBe """{"datetime":"1970-01-01T00:00:01Z"}"""
            }
        }

        "source with default int32 value" {
            processorNoDefaults.process(
                    TestProto.Int32ValueMessage.newBuilder()
                            .setSource(TestProto.Int32ValueMessage.Source.newBuilder()
                                    .setNullableInt(Int32Value.newBuilder().setValue(0))),
                    "test_index"
            ).run {
                source shouldBe """{"nullable_int":0}"""
            }
        }

        "missing int32 value" {
            processorNoDefaults.process(
                    TestProto.Int32ValueMessage.getDefaultInstance(), "test_index"
            ).run {
                source shouldBe "{}"
            }
        }
    }
}
