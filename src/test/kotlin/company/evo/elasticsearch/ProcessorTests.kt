package company.evo.elasticsearch

import com.google.gson.Gson
import com.google.protobuf.Int32Value
import com.google.protobuf.Struct
import com.google.protobuf.Timestamp
import com.google.protobuf.Value

import org.assertj.core.api.Assertions.*

import org.junit.jupiter.api.Test

import company.evo.kafka.TestProto
import company.evo.kafka.elasticsearch.BulkActionProto.BulkAction
import company.evo.kafka.elasticsearch.BulkActionProto.Script


class JsonProcessorTests {
    @Test
    fun testEmptyMap() {
        assertThatThrownBy { processJsonMessage(mapOf<Any,Any?>(), "test_index") }
                .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun testInvalidPayload() {
        assertThatThrownBy {
            processJsonMessage(mapOf("payload" to "<invalid>"), "test_index")
        }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("[payload]")
    }

    @Test
    fun testMissingAction() {
        assertThatThrownBy {
            processJsonMessage(mapOf("source" to mapOf<Any,Any?>()), "test_index")
        }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("[action]")
    }

    @Test
    fun testMissingSource() {
        assertThatThrownBy {
            processJsonMessage(
                    mapOf(
                            "action" to mapOf(
                                    "index" to mapOf<Any,Any?>()
                            )
                    ),
                    "test_index"
            )
        }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("[source]")
    }

    @Test
    fun testUnknownAction() {
        assertThatThrownBy {
            processJsonMessage(
                    mapOf(
                            "action" to mapOf(
                                    "<unknown>" to mapOf<Any,Any?>()
                            )
                    ),
                    "test_index"
            )
        }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("[index, create, update, delete]")
    }

    @Test
    fun testIndex() {
        val action = processJsonMessage(
                mapOf<Any, Any?>(
                        "action" to mapOf(
                                "index" to mapOf(
                                        "id" to 123,
                                        "type" to "test")
                        ),
                        "source" to mapOf(
                                "name" to "Teo"
                        )),
                "test_index")
        assertThat(action)
                .hasFieldOrPropertyWithValue("index", "test_index")
                .hasFieldOrPropertyWithValue("type", "test")
                .hasFieldOrPropertyWithValue("id", "123")
                .hasFieldOrPropertyWithValue("bulkMethodName", "index")
                .returns(
                        """{"name":"Teo"}""",
                        {
                            (it as AnyBulkableAction).getData(Gson())
                        }
                )
                .returns(
                        listOf(),
                        {
                            (it as AnyBulkableAction)
                                    .getParameter("routing").toList()
                        }
                )
    }

    @Test
    fun testDelete() {
        val action = processJsonMessage(
                mapOf<Any, Any?>(
                        "action" to mapOf(
                                "delete" to mapOf(
                                        "id" to 123,
                                        "type" to "test"
                                )
                        )
                ),
                "test_index")
        assertThat(action)
                .hasFieldOrPropertyWithValue("index", "test_index")
                .hasFieldOrPropertyWithValue("type", "test")
                .hasFieldOrPropertyWithValue("id", "123")
                .hasFieldOrPropertyWithValue("bulkMethodName", "delete")
                .returns(
                        null,
                        {
                            (it as AnyBulkableAction).getData(Gson())
                        }
                )
    }

    @Test
    fun testUpdate() {
        val action = processJsonMessage(
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
                "test_index")
        assertThat(action)
                .hasFieldOrPropertyWithValue("index", "test_index")
                .hasFieldOrPropertyWithValue("type", "test")
                .hasFieldOrPropertyWithValue("id", "123")
                .hasFieldOrPropertyWithValue("bulkMethodName", "update")
                .returns(
                        """{"doc":{"name":"Updated name"}}""",
                        {
                            (it as AnyBulkableAction)
                                    .getData(Gson())
                        }
                )
    }


    @Test
    fun testCreate() {
        val action = processJsonMessage(
                mapOf<Any, Any?>(
                        "action" to mapOf(
                                "create" to mapOf(
                                        "id" to 123,
                                        "type" to "test")
                        ),
                        "source" to mapOf(
                                "name" to "Teo"
                        )),
                "test_index")
        assertThat(action)
                .hasFieldOrPropertyWithValue("index", "test_index")
                .hasFieldOrPropertyWithValue("type", "test")
                .hasFieldOrPropertyWithValue("id", "123")
                .hasFieldOrPropertyWithValue("bulkMethodName", "create")
                .returns(
                        """{"name":"Teo"}""",
                        {
                            (it as AnyBulkableAction).getData(Gson())
                        }
                )
    }
}

class ProtobufProcessorTests {
    private val gson = Gson()
    private val testMsg = TestProto.TestMessage.newBuilder()
            .setAction(BulkAction.newBuilder()
                    .setOpType(BulkAction.OpType.INDEX)
                    .setType("test")
                    .setId("123")
                    .setRouting("4"))
            .setSource(TestProto.TestDocument.newBuilder()
                    .setId(0)
                    .setName("Teo")
                    .build())
            .build()

    @Test
    fun missingAction() {
        assertThatThrownBy {
            processProtobufMessage(
                    TestProto.MissingActionMessage.getDefaultInstance(), "test_index",
                    includeDefaultValues = true
            )
        }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("[action]")
    }

    @Test
    fun invalidAction() {
        assertThatThrownBy {
            processProtobufMessage(
                    TestProto.InvalidActionMessage.getDefaultInstance(), "test_index",
                    includeDefaultValues = true
            )
        }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("[action]")
                .hasMessageContaining("BulkAction")
    }

    @Test
    fun missingSource() {
        assertThatThrownBy {
            processProtobufMessage(
                    TestProto.MissingSourceMessage.getDefaultInstance(), "test_index",
                    includeDefaultValues = true
            )
        }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("[source]")
    }

    @Test
    fun invalidSource() {
        assertThatThrownBy {
            processProtobufMessage(
                    TestProto.InvalidSourceMessage.getDefaultInstance(), "test_index",
                    includeDefaultValues = true
            )
        }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("[source]")
                .hasMessageContaining("Message")
    }

    @Test
    fun testIndex() {
        val action = processProtobufMessage(testMsg, "test_index")
        assertThat(action.index).isEqualTo("test_index")
        assertThat(action.type).isEqualTo("test")
        assertThat(action.id).isEqualTo("123")
        assertThat(action.getParameter("routing")).containsOnly("4")
        assertThat(action.getParameter("parent")).isEmpty()
        assertThat(action.bulkMethodName).isEqualTo("index")
        val data = action.getData(gson)
        assertThat(data).isEqualTo(
                """{"name":"Teo"}"""
        )
    }

    @Test
    fun testIndexIncludingDefaultValues() {
        val action = processProtobufMessage(
                testMsg, "test_index", includeDefaultValues = true)
        assertThat(action.index).isEqualTo("test_index")
        assertThat(action.type).isEqualTo("test")
        assertThat(action.id).isEqualTo("123")
        assertThat(action.getParameter("routing")).containsOnly("4")
        assertThat(action.getParameter("parent")).isEmpty()
        assertThat(action.bulkMethodName).isEqualTo("index")
        val gson = Gson()
        val data = action.getData(gson)
        assertThat(data).isEqualTo(
                """{"id":0,"name":"Teo","counter":"0"}"""
        )
    }

    @Test
    fun testDelete() {
        val action = processProtobufMessage(
                TestProto.TestMessage.newBuilder()
                        .setAction(BulkAction.newBuilder()
                                .setOpType(BulkAction.OpType.DELETE)
                                .setType("test").setId("123").setRouting("4")
                        ),
                "test_index"
        )
        assertThat(action.index).isEqualTo("test_index")
        assertThat(action.type).isEqualTo("test")
        assertThat(action.id).isEqualTo("123")
        assertThat(action.getParameter("routing")).containsOnly("4")
        assertThat(action.getParameter("parent")).isEmpty()
        assertThat(action.bulkMethodName).isEqualTo("delete")
        assertThat(action.getData(gson)).isNull()
    }

    @Test
    fun testUpdate() {
        val action = processProtobufMessage(
                TestProto.UpdateMessage.newBuilder()
                        .setAction(BulkAction.newBuilder()
                                .setOpType(BulkAction.OpType.UPDATE)
                                .setType("test").setId("123").setRouting("4")
                        )
                        .setSource(TestProto.UpdateMessage.Source.newBuilder()
                                .setDoc(TestProto.TestDocument.newBuilder()
                                        .setName("Leo"))
                                .setDocAsUpsert(true)),
                "test_index"
        )
        assertThat(action.index).isEqualTo("test_index")
        assertThat(action.type).isEqualTo("test")
        assertThat(action.id).isEqualTo("123")
        assertThat(action.getParameter("routing")).containsOnly("4")
        assertThat(action.getParameter("parent")).isEmpty()
        assertThat(action.bulkMethodName).isEqualTo("update")
        assertThat(action.getData(gson)).isEqualTo(
                """{"doc":{"name":"Leo"},"doc_as_upsert":true}"""
        )
    }

    @Test
    fun testUpdateScript() {
        val action = processProtobufMessage(
                TestProto.UpdateMessage.newBuilder()
                        .setAction(BulkAction.newBuilder()
                                .setOpType(BulkAction.OpType.UPDATE)
                                .setType("test").setId("123").setRouting("4")
                        )
                        .setSource(TestProto.UpdateMessage.Source.newBuilder()
                                .setScript(Script.newBuilder()
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
        )
        assertThat(action.index).isEqualTo("test_index")
        assertThat(action.type).isEqualTo("test")
        assertThat(action.id).isEqualTo("123")
        assertThat(action.getParameter("routing")).containsOnly("4")
        assertThat(action.getParameter("parent")).isEmpty()
        assertThat(action.bulkMethodName).isEqualTo("update")
        assertThat(action.getData(gson)).isEqualTo(
                """{"script":{"lang":"painless","source":"ctx._source.counter += params.count","params":{"count":4.0}},"upsert":{"name":"Teo","counter":"1"}}"""
        )
    }

    @Test
    fun testCreate() {
        val action = processProtobufMessage(
                TestProto.TestMessage.newBuilder()
                        .setAction(BulkAction.newBuilder()
                                .setOpType(BulkAction.OpType.CREATE)
                                .setType("test").setId("123").setRouting("4")
                        )
                        .setSource(TestProto.TestDocument.newBuilder()
                                .setName("Leo")),
                "test_index"
        )
        assertThat(action.index).isEqualTo("test_index")
        assertThat(action.type).isEqualTo("test")
        assertThat(action.id).isEqualTo("123")
        assertThat(action.getParameter("routing")).containsOnly("4")
        assertThat(action.getParameter("parent")).isEmpty()
        assertThat(action.bulkMethodName).isEqualTo("create")
        assertThat(action.getData(gson)).isEqualTo(
                """{"name":"Leo"}"""
        )
    }

    @Test
    fun testEnum() {
        val action = processProtobufMessage(
                TestProto.EnumMessage.newBuilder()
                        .setSource(TestProto.EnumMessage.Source.newBuilder()
                                .setStatus(TestProto.EnumMessage.Status.DELETED)),
                "test_index"
        )
        assertThat(action.getData(gson))
                .isEqualTo("""{"status":"DELETED"}""")
    }

    @Test
    fun testEnumDefault() {
        val action = processProtobufMessage(
                TestProto.EnumMessage.newBuilder()
                        .setSource(TestProto.EnumMessage.Source.getDefaultInstance()),
                "test_index"
        )
        assertThat(action.getData(gson)).isEqualTo("{}")
    }

    @Test
    fun testEnumIncludingDefaultValues() {
        val action = processProtobufMessage(
                TestProto.EnumMessage.newBuilder()
                        .setSource(TestProto.EnumMessage.Source.getDefaultInstance()),
                "test_index",
                includeDefaultValues = true
        )
        assertThat(action.getData(gson))
                .isEqualTo("""{"status":"ACTIVE"}""")
    }

    @Test
    fun testRepeated() {
        val action = processProtobufMessage(
                TestProto.RepeatedMessage.newBuilder()
                        .setSource(TestProto.RepeatedMessage.Source.newBuilder()
                                .addAllDeliveryRegions(listOf(1, 5))),
                "test_index"
        )
        assertThat(action.getData(gson))
                .isEqualTo("""{"delivery_regions":[1,5]}""")
    }

    @Test
    fun testEmptyRepeated() {
        val action = processProtobufMessage(
                TestProto.RepeatedMessage.newBuilder()
                        .setSource(TestProto.RepeatedMessage.Source.getDefaultInstance()),
                "test_index"
        )
        assertThat(action.getData(gson)).isEqualTo("{}")
    }

    @Test
    fun testMap() {
        val action = processProtobufMessage(
                TestProto.MapMessage.newBuilder()
                        .setSource(TestProto.MapMessage.Source.newBuilder()
                                .putMyMap("test", "test value")),
                "test_index"
        )
        assertThat(action.getData(gson))
                .isEqualTo("""{"my_map":{"test":"test value"}}""")
    }

    @Test
    fun testEmptyMap() {
        val action = processProtobufMessage(
                TestProto.MapMessage.newBuilder()
                        .setSource(TestProto.MapMessage.Source.getDefaultInstance()),
                "test_index"
        )
        assertThat(action.getData(gson)).isEqualTo("{}")
    }

    @Test
    fun testEmptyMapIncludingDefaultValues() {
        val action = processProtobufMessage(
                TestProto.MapMessage.newBuilder()
                        .setSource(TestProto.MapMessage.Source.getDefaultInstance()),
                "test_index",
                includeDefaultValues = true
        )
        assertThat(action.getData(gson)).isEqualTo("""{"my_map":{}}""")
    }

    @Test
    fun testTimestamp() {
        val action = processProtobufMessage(
                TestProto.DatetimeMessage.newBuilder()
                        .setSource(TestProto.DatetimeMessage.Source.newBuilder()
                                .setDatetime(Timestamp.newBuilder().setSeconds(1))),
                "test_index"
        )
        assertThat(action.getData(gson))
                .isEqualTo("""{"datetime":"1970-01-01T00:00:01Z"}""")
    }

    @Test
    fun testDefaultInt32Value() {
        val action = processProtobufMessage(
                TestProto.Int32ValueMessage.newBuilder()
                        .setSource(TestProto.Int32ValueMessage.Source.newBuilder()
                                .setNullableInt(Int32Value.newBuilder().setValue(0))),
                "test_index"
        )
        assertThat(action.getData(gson)).isEqualTo("""{"nullable_int":0}""")
    }

    @Test
    fun testMissingInt32Value() {
        val action = processProtobufMessage(
                TestProto.Int32ValueMessage.getDefaultInstance(), "test_index"
        )
        assertThat(action.getData(gson)).isEqualTo("{}")
    }
}
