package company.evo.elasticsearch

import io.searchbox.action.*
import io.searchbox.client.JestClient
import io.searchbox.client.JestClientFactory
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.*
import io.searchbox.params.Parameters
import org.apache.http.conn.HttpHostConnectException

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.RetriableCommitFailedException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import org.slf4j.LoggerFactory


class ElasticsearchSinkTask : SinkTask() {
    lateinit private var esClient: JestClient
    lateinit private var topicToIndexMap: Map<String, String>
    private val buffer = ArrayList<AnyBulkableAction>()

    companion object {
        val logger = LoggerFactory.getLogger(ElasticsearchSinkTask::class.java)
    }

    override fun start(props: MutableMap<String, String>) {
        logger.debug("Starting ElasticsearchSinkTask")
        try {
            val config = Config(props)
            this.topicToIndexMap = config.getMap(Config.TOPIC_INDEX_MAP)
            val esClientFactory = JestClientFactory()
            esClientFactory.setHttpClientConfig(
                    HttpClientConfig.Builder(config.getList(Config.CONNECTION_URL))
                            .build())
            this.esClient = esClientFactory.`object`
        } catch (e: ConfigException) {
            throw ConnectException(
                    "Couldn't start ElasticsearchSinkTask due to configuration error: $e"
            )
        }
    }

    override fun stop() {
        logger.debug("Stopping ElasticsearchSinkTask")
        esClient.close()
    }

    override fun version(): String {
        return "unknown"
    }

    override fun put(records: MutableCollection<SinkRecord>) {
        records.forEach { record ->
            val index = topicToIndexMap[record.topic()]
            val value = record.value()
            try {
                when (value) {
                    is Map<*,*> -> {
                        buffer.add(processMapMessage(value, index))
                    }
                    else -> {
                        throw IllegalArgumentException("Expected Map but was: ${value.javaClass}")
                    }
                }
            } catch (e: IllegalArgumentException) {
                logger.error("Malformed message: $e")
            }
        }
    }

    override fun flush(currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata>?) {
        if (buffer.isEmpty()) {
            super.flush(currentOffsets)
            return
        }
        val bulkRequest = Bulk.Builder()
                .defaultIndex("test")
                .addAction(buffer)
                .build()
        try {
            val bulkResult = esClient.execute(bulkRequest)
            if (!bulkResult.isSucceeded) {
                var errorMsg = ""
                // TODO(Fix Jest to correctly process missing id)
                bulkResult.failedItems.forEach {
                    if (it.error != null) {
                        errorMsg += "\t${it.id}: ${it.error}\n"
                        // TODO(Parse error message and determine possibility of retry)
                    }
                }
                buffer.clear()
                throw RetriableCommitFailedException(
                        "Some documents weren't indexed:\n${errorMsg}"
                )
            }
            buffer.clear()
            super.flush(currentOffsets)
        } catch (e: HttpHostConnectException) {
            buffer.clear()
            throw RetriableCommitFailedException("$e")
        }
    }

    private fun processMapMessage(value: Map<*,*>, index: String?): AnyBulkableAction {
        val payload = value["payload"]
        when (payload) {
            is Map<*,*> -> {
                val actionData = payload["action"]
                val sourceData = payload["source"]
                when {
                    actionData is Map<*,*> && sourceData is Map<*,*> -> {
                        val actionBuilder = AnyBulkableAction.Builder(actionData)
                        val actionEntry = actionData.iterator().next()
                        when (actionEntry.key) {
                            "index", "create", "update" -> {
                                actionBuilder.setSource(sourceData)
                            }
                            "delete" -> {}
                            else -> {
                                throw IllegalArgumentException(
                                        "Expected one of action [index, create, update, delete] " +
                                        "but was [${actionEntry.key}]")
                            }
                        }
                        if (index != null) {
                            actionBuilder.index(index)
                        }
                        return actionBuilder.build()
                    }
                    else -> {
                        throw IllegalArgumentException(
                                "Expected Map's but were: " +
                                "[action: ${actionData?.javaClass}, source: ${sourceData?.javaClass}]")
                    }
                }
            }
            else -> {
                throw IllegalArgumentException("Expected Map but was: ${payload?.javaClass}")
            }
        }
    }
}

private class AnyBulkableAction(builder: Builder) :
        SingleResultAbstractDocumentTargetedAction(builder),
        BulkableAction<DocumentResult>
{
    val opType = builder.opType

    override fun getBulkMethodName(): String = opType

    override fun getRestMethodName(): String {
        return when (bulkMethodName) {
            "create" -> { "PUT" }
            "delete" -> { "DELETE" }
            else -> { "POST" }
        }
    }

    class Builder(action: Map<*,*>) :
            AbstractDocumentTargetedAction.Builder<AnyBulkableAction, Builder>()
    {
        val opType: String
        var source: Map<*,*>? = null

        init {
            val actionEntry = action.iterator().next()
            this.opType = castOrFail(actionEntry.key)
            val params = castOrFail<Map<*,*>>(actionEntry.value)
            val index = params["_index"] ?: params["index"]
            if (index != null) {
                this.index(castOrFail(index))
            }
            val type = params["_type"] ?: params["type"]
            if (type != null) {
                this.type(castOrFail(type))
            }
            val id = params["_id"] ?: params["id"]
            if (id != null) {
                this.id(id.toString())
            }
            Parameters.ACCEPTED_IN_BULK.forEach {
                val paramWithUnderscore = "_$it"
                val paramValue = params[paramWithUnderscore] ?: params[it]
                if (paramValue != null) {
                    setParameter(it, paramValue)
                }
            }
        }

        private inline fun <reified T> castOrFail(obj: Any?): T {
            return obj as? T ?:
                    throw IllegalArgumentException(
                            "Expected ${T::class.java} class but was: ${obj?.javaClass}")
        }

        fun setSource(source: Map<*,*>): Builder {
            this.source = source
            return this
        }

        override fun build(): AnyBulkableAction {
            return AnyBulkableAction(this)
        }
    }
}
