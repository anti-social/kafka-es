package company.evo.bulk.elasticsearch

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

import io.kotlintest.Description
import io.kotlintest.data.forall
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.kotlintest.tables.row

import java.io.ByteArrayOutputStream
import java.lang.IllegalArgumentException

class ElasticBulkSerializationTests : StringSpec() {
    private val mapper = jacksonObjectMapper()
    private val outStream = ByteArrayOutputStream()

    override fun beforeTest(description: Description) {
        super.beforeTest(description)
        outStream.reset()
    }

    init {
        "delete bulk action" {
            BulkAction(BulkAction.Operation.DELETE, "test", "_doc", "1", routing = "2")
                    .writeTo(mapper, outStream)
            outStream.toByteArray().toString(Charsets.UTF_8) shouldBe """
                |{"delete":{"_index":"test","_type":"_doc","_id":"1","routing":"2"}}
                |""".trimMargin()
        }

        "index bulk action" {
            BulkAction(
                    BulkAction.Operation.INDEX, "test", "_doc", "1",
                    source = mapOf("name" to "Test document", "status" to 1)
            )
                    .writeTo(mapper, outStream)
            outStream.toByteArray().toString(Charsets.UTF_8) shouldBe """
                |{"index":{"_index":"test","_type":"_doc","_id":"1"}}
                |{"name":"Test document","status":1}
                |""".trimMargin()
        }

        "delete with source" {
            shouldThrow<IllegalArgumentException> {
                BulkAction(
                        BulkAction.Operation.DELETE, "test", "_doc", "1",
                        source = mapOf("name" to "Test")
                )
            }
        }

        "operations that must have source" {
            forall(
                    row(BulkAction.Operation.INDEX),
                    row(BulkAction.Operation.UPDATE),
                    row(BulkAction.Operation.CREATE)
            ) { operation ->
                shouldThrow<IllegalArgumentException> {
                    BulkAction(
                            operation, "test", "_doc", "1"
                    )
                }
            }
        }
    }
}
