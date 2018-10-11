package company.evo.kafka.elasticsearch

import io.searchbox.action.*
import io.searchbox.core.*
import io.searchbox.params.Parameters

import company.evo.kafka.elasticsearch.BulkActionProto.BulkAction
import company.evo.castOrFail


class AnyBulkableAction private constructor(builder: RealBuilder) :
        SingleResultAbstractDocumentTargetedAction(builder),
        BulkableAction<DocumentResult>
{
    val opType = builder.opType

    init {
        this.payload = builder.source
    }

    override fun getBulkMethodName(): String = opType

    override fun getRestMethodName(): String {
        return when (bulkMethodName) {
            "create" -> { "PUT" }
            "delete" -> { "DELETE" }
            else -> { "POST" }
        }
    }

    fun getSource() = payload

    class Builder {
        private val builder = RealBuilder()

        constructor(action: Map<*, *>) : super() {
            if (action.isEmpty()) {
                throw IllegalArgumentException("Empty action")
            }
            val actionEntry = action.iterator().next()
            builder.opType = castOrFail(actionEntry.key)
            val params = castOrFail<Map<*, *>>(actionEntry.value)
            val index = params["_index"] ?: params["index"]
            if (index != null) {
                builder.index(castOrFail(index))
            }
            val type = params["_type"] ?: params["type"]
            if (type != null) {
                builder.type(castOrFail(type))
            }
            val id = params["_id"] ?: params["id"]
            if (id != null) {
                builder.id(id.toString())
            }
            Parameters.ACCEPTED_IN_BULK.forEach {
                val paramWithUnderscore = "_$it"
                val paramValue = params[paramWithUnderscore] ?: params[it]
                if (paramValue != null) {
                    builder.setParameter(it, paramValue)
                }
            }
        }

        constructor(action: BulkAction) : super() {
            builder.opType = action.opType.toString().toLowerCase()
            if (!action.index.isEmpty()) {
                index(action.index)
            }
            if (!action.type.isEmpty()) {
                builder.type(action.type)
            }
            if (!action.id.isEmpty()) {
                builder.id(action.id)
            }
            if (!action.routing.isEmpty()) {
                builder.setParameter(Parameters.ROUTING, action.routing)
            }
            if (!action.parent.isEmpty()) {
                builder.setParameter(Parameters.PARENT, action.parent)
            }
            if (!action.version.isEmpty()) {
                builder.setParameter(Parameters.VERSION, action.version)
            }
            if (!action.versionType.isEmpty()) {
                builder.setParameter(Parameters.VERSION_TYPE, action.versionType)
            }
            if (action.retryOnConflict != 0) {
                builder.setParameter(Parameters.RETRY_ON_CONFLICT, action.retryOnConflict)
            }
        }

        fun index(index: String): Builder {
            builder.index(index)
            return this
        }

        fun setSource(source: Any): Builder {
            builder.setSource(source)
            return this
        }

        fun build(): AnyBulkableAction {
            return builder.build()
        }
    }

    private class RealBuilder :
            AbstractDocumentTargetedAction.Builder<AnyBulkableAction, RealBuilder>()
    {
        lateinit var opType: String
        var source: Any? = null

        fun setSource(source: Any): RealBuilder {
            this.source = source
            return this
        }

        override fun build(): AnyBulkableAction {
            return AnyBulkableAction(this)
        }
    }
}
