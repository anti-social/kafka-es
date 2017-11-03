package company.evo.elasticsearch

import com.google.gson.Gson

import com.google.protobuf.Timestamp

import org.assertj.core.api.Assertions.*

import org.junit.jupiter.api.Test

import company.evo.kafka.elasticsearch.BulkActionProto.BulkAction
import company.evo.kafka.TestMessageProto.TestDocument
import company.evo.kafka.TestMessageProto.TestMessage


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
    private val testMsg = TestMessage.newBuilder()
            .setAction(BulkAction.newBuilder()
                    .setOpType(BulkAction.OpType.INDEX)
                    .setType("test")
                    .setId("123")
                    .setRouting("4"))
            .setSource(TestDocument.newBuilder()
                    .setName("Teo")
                    .setStatus(TestDocument.Status.ACTIVE)
                    .setDateCreated(Timestamp.newBuilder()
                            .setSeconds(1))
                    .putMyMap("test", "test")
                    .build())
            .build()

    @Test
    fun testIndexIncludingDefaultValues() {
        assertThat(testMsg.source.status).isEqualTo(TestDocument.Status.ACTIVE)
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
                """{"id":"0","name":"Teo","status":"ACTIVE","tags":[],"date_created":"1970-01-01T00:00:01Z","my_map":{"test":"test"}}"""
        )
    }

    @Test
    fun testIndexWithoutDefaultValues() {
        assertThat(testMsg.source.status).isEqualTo(TestDocument.Status.ACTIVE)
        val action = processProtobufMessage(
                testMsg, "test_index", includeDefaultValues = false)
        assertThat(action.index).isEqualTo("test_index")
        assertThat(action.type).isEqualTo("test")
        assertThat(action.id).isEqualTo("123")
        assertThat(action.getParameter("routing")).containsOnly("4")
        assertThat(action.getParameter("parent")).isEmpty()
        assertThat(action.bulkMethodName).isEqualTo("index")
        val gson = Gson()
        val data = action.getData(gson)
        assertThat(data).isEqualTo(
                """{"name":"Teo","date_created":"1970-01-01T00:00:01Z","my_map":{"test":"test"}}"""
        )
    }
}
