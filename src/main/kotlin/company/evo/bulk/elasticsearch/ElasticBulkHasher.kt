package company.evo.bulk.elasticsearch

import company.evo.bulk.Hasher

class ElasticBulkHasher : Hasher<BulkAction> {
    override fun hash(obj: BulkAction): Int {
        // TODO Should we hash by routing key?
        return obj.id.hashCode() and 0x7FFF_FFFF
    }
}
