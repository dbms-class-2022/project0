package net.barashev.dbi2022.fake

import net.barashev.dbi2022.*
import java.util.function.Function

class FakeIndex<S: AttributeType<T>, T: Comparable<T>>(
    private val accessMethodManager: AccessMethodManager,
    private val pageCache: PageCache,
    private val tableName: String,
    private val indexTableName: String,
    private val keyType: S,
    private val indexKey: Function<ByteArray, T>
): Index<T> {

    private val key2page = mutableMapOf<T, PageId>()

    init {
        try  {
            accessMethodManager.pageCount(tableName)
            openIndex()
        }
        catch (ex: AccessMethodException) {
            buildIndex()
        }
    }

    private fun buildIndex() {
        val indexTableOid = accessMethodManager.createTable(indexTableName)
        val createIndexPage = {
            pageCache.getAndPin(accessMethodManager.addPage(indexTableOid))
        }
        var outPage = createIndexPage()
        accessMethodManager.createFullScan(tableName) {
            indexKey.apply(it)
        }.pages().forEach { page ->
            page.allRecords().forEach { rid, result ->
                if (result.isOk) {
                    val indexKey = indexKey.apply(result.bytes)
                    val indexRecord = Record2(keyType to indexKey, intField(page.id))
                    if (outPage.putRecord(indexRecord.asBytes()).isOutOfSpace) {
                        outPage.close()
                        outPage = createIndexPage()
                        outPage.putRecord(indexRecord.asBytes())
                    }
                    key2page[indexKey] = page.id
                }
            }
        }
        outPage.close()
    }

    private fun openIndex() {
        val parser = { bytes: ByteArray -> Record2(keyType to keyType.defaultValue(), intField()).fromBytes(bytes)}
        accessMethodManager.createFullScan(indexTableName, parser).pages().forEach {
            it.allRecords().forEach { rid, res ->
                if (res.isOk) {
                    parser(res.bytes).let {
                        key2page[it.value1] = it.value2
                    }
                }
            }
        }
    }

    override fun lookup(indexKey: T): PageId? = key2page[indexKey]
}

class FakeIndexFactory(private val accessMethodManager: AccessMethodManager, private val pageCache: PageCache):
    IndexFactory {
    override fun <T : Comparable<T>, S : AttributeType<T>> build(
        tableName: String,
        indexTableName: String,
        method: IndexMethod,
        keyType: S,
        indexKey: Function<ByteArray, T>
    ): Index<T> = FakeIndex(accessMethodManager, pageCache, tableName, indexTableName, keyType, indexKey)

    override fun <T : Comparable<T>, S : AttributeType<T>> open(
        tableName: String,
        indexTableName: String,
        method: IndexMethod,
        keyType: S,
        indexKey: Function<ByteArray, T>
    ): Index<T> = FakeIndex(accessMethodManager, pageCache, tableName, indexTableName, keyType, indexKey)

}