package company.evo.elasticsearch

import kotlin.test.assertEquals
import org.junit.Test


class ElasticsearchSinkTaskTests {
    @Test fun test() {
        val task = ElasticsearchSinkTask()
        task.start(null)
        task.put(arrayListOf())
        task.flush(null)
    }
}
