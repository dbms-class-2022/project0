import java.lang.IllegalArgumentException
import java.util.function.Function

typealias Oid = Int
typealias OidPageidRecord = Record2<Oid, PageId>
typealias OidNameRecord = Record2<Oid, String>
typealias TableAttributeRecord = Record3<Oid, String, Int>

private const val ATTRIBUTE_SYSTABLE_OID = 1

const val NAME_SYSTABLE_OID = 0

class SystemCatalogException(message: String): Exception(message)

class TableOidMapping(
    private val pageCache: PageCache,
    private val tablePageDirectory: TablePageDirectory) {
    private val cachedMapping = mutableMapOf<String, Oid?>()

    private fun createAccess(): FullScanAccessImpl<OidNameRecord> =
        FullScanAccessImpl(pageCache, NAME_SYSTABLE_OID, tablePageDirectory.records(NAME_SYSTABLE_OID).iterator()) {
            OidNameRecord(intField(), stringField()).fromBytes(it)
        }

    fun get(tableName: String): Oid? {
        val oid = cachedMapping.getOrPut(tableName) {
            createAccess().firstOrNull {
                it.value2 == tableName
            }?.value1 ?: -1
        }
        return if (oid == -1) null else oid
    }

    fun create(tableName: String): Oid {
        val nextOid = nextTableOid()
        val record = OidNameRecord(intField(), stringField(tableName))
        val bytes = record.asBytes()
        val availablePageId = createAccess().iteratorImpl().seekFirstPage {
            it.diskPage.freeSpace > bytes.size + it.diskPage.recordHeaderSize
        }?.diskPage?.id ?: tablePageDirectory.add(NAME_SYSTABLE_OID)
        pageCache.getAndPin(availablePageId).use {page ->
            page.diskPage.putRecord(bytes)
        }
        cachedMapping[tableName] = nextOid
        return nextOid
    }

    private fun nextTableOid(): Oid {
        var maxOid = NAME_SYSTABLE_OID + 1
        createAccess().forEach {
            maxOid = maxOf(maxOid, it.value1)
        }
        return maxOid
    }

}


class DumbSystemCatalogImpl(private val pageCache: PageCache, private val storage: Storage) {
    private val tablePageDirectory = SimplePageDirectoryImpl(pageCache)
    private val tableOidMapping = TableOidMapping(pageCache, tablePageDirectory)

    private fun <T> createFullScan(tableOid: Oid, recordBytesParser: Function<ByteArray, T>) =
        FullScanAccessImpl(pageCache, tableOid, RootRecordIteratorImpl(pageCache, tableOid, 1), recordBytesParser)

    fun <T> createFullScan(tableName: String, recordBytesParser: Function<ByteArray, T>) =
        tableOidMapping.get(tableName)
            ?.let { tableOid -> createFullScan(tableOid, recordBytesParser) }
            ?: throw SystemCatalogException("Relation $tableName not found")

    fun createTable(tableName: String, vararg columns: Triple<String, AttributeType<Any>, ColumnConstraint?>): Oid {
        if (tableOidMapping.get(tableName) != null) {
            throw IllegalArgumentException("Table $tableName already exists")
        }
        return tableOidMapping.create(tableName)
    }

    fun addPage(tableOid: Oid): PageId = tablePageDirectory.add(tableOid)


}