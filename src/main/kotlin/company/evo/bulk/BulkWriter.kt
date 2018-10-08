package company.evo.bulk

class BulkWriteException(msg: String, cause: Throwable? = null) : Exception(msg, cause)

interface BulkWriter<in T> {
    suspend fun write(actions: List<T>): Boolean
}
