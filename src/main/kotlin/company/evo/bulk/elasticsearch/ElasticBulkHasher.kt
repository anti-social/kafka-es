package company.evo.bulk.elasticsearch

import company.evo.bulk.Hasher

class ElasticBulkHasher : Hasher<BulkAction> {
    private var ix = 0

    override fun hash(obj: BulkAction): Int {
        val hash = obj.routing?.hashCode()
                ?: obj.id.hashCode()
                ?: ix++
        return hash and 0x7FFF_FFFF
    }
}
