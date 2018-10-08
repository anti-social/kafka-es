package company.evo.bulk.elasticsearch

import io.kotlintest.Description
import io.kotlintest.Spec
import io.kotlintest.TestCaseConfig
import io.kotlintest.TestResult
import io.kotlintest.matchers.containExactly
import io.kotlintest.should
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec

import kotlinx.coroutines.runBlocking

import org.apache.http.HttpHost
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.get.GetIndexRequest
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.search.fetch.subphase.FetchSourceContext

class ElasticBulkWriterTests : StringSpec() {
    companion object {
        const val ES6_URL = "http://localhost:9206"
        const val TEST_INDEX_NAME = "test"
    }

    private val httpClient = autoClose(
            HttpAsyncClientBuilder.create().build()
    )
    private val esClient = autoClose(
            RestHighLevelClient(RestClient.builder(
                    HttpHost("localhost", 9206)
            ))
    )

    override val defaultTestCaseConfig = TestCaseConfig(tags = setOf(company.evo.Integration))

    override fun beforeSpec(description: Description, spec: Spec) {
        super.beforeSpec(description, spec)
        httpClient.start()
    }

    override fun beforeTest(description: Description) {
        super.beforeTest(description)
        deleteIndexIfExists()
        createIndex()
    }

    override fun afterTest(description: Description, result: TestResult) {
        super.afterTest(description, result)
        deleteIndexIfExists()
    }

    private fun deleteIndexIfExists() {
        if (esClient.indices().exists(GetIndexRequest().indices(TEST_INDEX_NAME), RequestOptions.DEFAULT)) {
            esClient.indices().delete(DeleteIndexRequest(TEST_INDEX_NAME), RequestOptions.DEFAULT)
        }
    }

    private fun createIndex() {
        esClient.indices().create(
                CreateIndexRequest(
                        TEST_INDEX_NAME,
                        Settings.builder()
                                .put("number_of_shards", 1)
                                .put("number_of_replicas", 0)
                                .build()
                ),
                RequestOptions.DEFAULT
        )
    }

    private fun refreshIndex() {
        esClient.indices().refresh(RefreshRequest(TEST_INDEX_NAME), RequestOptions.DEFAULT)
    }

    private fun getDoc(id: String): GetResponse {
        return esClient.get(
                GetRequest(TEST_INDEX_NAME, "_doc", id)
                        .fetchSourceContext(FetchSourceContext.FETCH_SOURCE),
                RequestOptions.DEFAULT
        )
    }

    private fun addDoc(id: String, source: Map<*, *>) {
        esClient.index(
                IndexRequest(TEST_INDEX_NAME, "_doc", id).source(source),
                RequestOptions.DEFAULT
        )
    }

    init {
        "index action" {
            val bulkWriter = ElasticBulkWriter(httpClient, listOf(ES6_URL))
            val source = mapOf(
                    "name" to "Test document",
                    "status" to 0
            )
            runBlocking {
                val action = BulkAction(
                        BulkAction.Operation.INDEX,
                        index = TEST_INDEX_NAME,
                        type = "_doc",
                        id = "1",
                        source = source
                )
                bulkWriter.write(listOf(action)) shouldBe true
            }
            refreshIndex()

            val doc = getDoc("1")
            doc.id shouldBe "1"
            doc.source should containExactly(source)
        }

        "delete action" {
            addDoc("1", mapOf("name" to "Must be deleted"))

            val bulkWriter = ElasticBulkWriter(httpClient, listOf(ES6_URL))
            runBlocking {
                val action = BulkAction(
                        BulkAction.Operation.DELETE,
                        index = TEST_INDEX_NAME,
                        type = "_doc",
                        id = "1"
                )
                bulkWriter.write(listOf(action)) shouldBe true
            }
            refreshIndex()

            val doc = getDoc("1")
            doc.id shouldBe "1"
            doc.isExists shouldBe false
        }

        "create action" {
            val origSource = mapOf<String, Any>("name" to "Should not be rewritten")
            addDoc("1", origSource)

            val bulkWriter = ElasticBulkWriter(httpClient, listOf(ES6_URL))
            runBlocking {
                val action = BulkAction(
                        BulkAction.Operation.CREATE,
                        index = TEST_INDEX_NAME,
                        type = "_doc",
                        id = "1",
                        source = mapOf("name" to "The best name ever")
                )
                bulkWriter.write(listOf(action)) shouldBe false
            }
            refreshIndex()

            val doc = getDoc("1")
            doc.id shouldBe "1"
            doc.source should containExactly(origSource)
        }

        "update action" {
            addDoc("1", mapOf("name" to "Test name", "status" to 0, "counter" to 1))

            val bulkWriter = ElasticBulkWriter(httpClient, listOf(ES6_URL))
            runBlocking {
                val updateDoc = BulkAction(
                        BulkAction.Operation.UPDATE,
                        index = TEST_INDEX_NAME,
                        type = "_doc",
                        id = "1",
                        source = mapOf("doc" to mapOf("status" to 1))
                )
                val updateScript = BulkAction(
                        BulkAction.Operation.UPDATE,
                        index = TEST_INDEX_NAME,
                        type = "_doc",
                        id = "1",
                        source = mapOf("script" to mapOf("source" to "ctx._source.counter += 1"))
                )
                bulkWriter.write(listOf(updateDoc, updateScript)) shouldBe true
            }
            refreshIndex()

            val doc = getDoc("1")
            doc.id shouldBe "1"
            doc.sourceAsMap should containExactly(
                    mapOf("name" to "Test name", "status" to 1, "counter" to 2)
            )

        }
    }
}