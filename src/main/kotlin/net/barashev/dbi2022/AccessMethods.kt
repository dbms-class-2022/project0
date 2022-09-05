package net.barashev.dbi2022

import java.util.function.Function
import kotlin.math.max

class FullScanAccessImpl<T>(
    private val pageCache: PageCache,
    private val tableOid: Oid,
    private val rootRecords: Iterator<OidPageidRecord>,
    private val recordBytesParser: Function<ByteArray, T>): Iterable<T> {
    override fun iterator(): Iterator<T> = iteratorImpl()
    fun iteratorImpl() = FullScanIteratorImpl(pageCache, tableOid, rootRecords, recordBytesParser)
}
class FullScanIteratorImpl<T>(
    private val pageCache: PageCache,
    private val tableOid: Oid,
    private val rootRecords: Iterator<OidPageidRecord>,
    private val recordBytesParser: Function<ByteArray, T>): Iterator<T> {
    private var currentPage: CachedPage? = null
    private var currentRecordIdx = 0
    private var currentRecord: T? = null

    init {
        advance()
    }

    override fun hasNext() = currentRecord != null

    override fun next(): T = currentRecord!!.also { advance() }

    private fun advancePage(): CachedPage? {
        while (rootRecords.hasNext()) {
            val nextOidPageidRecord = rootRecords.next()
            if (nextOidPageidRecord.value1 == tableOid) {
                return pageCache.get(nextOidPageidRecord.value2)
            }
        }
        return null
    }


    private fun advance() {
        val nextPair = advanceRecord(currentPage, currentRecordIdx, recordBytesParser)
        if (nextPair == null) {
            val nextPage = advancePage()
            if (nextPage == null) {
                currentRecord = null
                return
            }
            currentPage = nextPage
            currentRecordIdx = -1
            advance()
            return
        } else {
            currentRecordIdx = nextPair.second
            currentRecord = nextPair.first
            return
        }
    }

    fun seekFirstPage(filter: java.util.function.Predicate<CachedPage>): CachedPage? {
        do {
            val page = advancePage() ?: return null
            if (filter.test(page)) {
                return page
            }
            currentPage = page
        } while (true)
    }
}

private const val MAX_ROOT_PAGE_COUNT = 4096

class RootRecordIteratorImpl(
    private val pageCache: PageCache,
    private val startRootPageId: PageId = 1,
    private val maxRootPageCount: Int = MAX_ROOT_PAGE_COUNT): Iterator<OidPageidRecord> {
    private var currentRecord: OidPageidRecord? = null
    private var currentRecordIdx: RecordId = -1
    private var currentRootPageId: PageId = 0
    private var currentPage: CachedPage? = null
    init {
        currentRootPageId = startRootPageId - 1
        advance()
    }

    override fun hasNext(): Boolean {
        return currentRecord != null
    }

    override fun next(): OidPageidRecord {
        return currentRecord!!.also { advance() }
    }

    private fun advance() {
        val nextPair = advanceRecord(currentPage, currentRecordIdx) {
            OidPageidRecord(intField(), intField()).fromBytes(it)
        }
        if (nextPair == null) {
            val nextPage = advancePage()
            if (nextPage == null) {
                currentRecord = null
                return
            }
            currentRecordIdx = -1
            advance()
            return
        } else {
            currentRecordIdx = nextPair.second
            currentRecord = nextPair.first
            return
        }
    }

    private fun advancePage(): CachedPage? {
        currentPage?.close()
        currentRootPageId += 1
        return if (currentRootPageId >= startRootPageId + maxRootPageCount) {
            null
        } else {
            currentPage = pageCache.getAndPin(currentRootPageId)
            currentPage
        }
    }
}

interface TablePageDirectory {
    fun records(tableOid: Oid): Iterable<OidPageidRecord>
    fun add(tableOid: Oid, pageid: PageId = -1): PageId
}

class SimplePageDirectoryImpl(private val pageCache: PageCache): TablePageDirectory {
    private var maxPageId: PageId = MAX_ROOT_PAGE_COUNT + 1
    override fun records(tableOid: Oid): Iterable<OidPageidRecord> = RootRecords(pageCache, tableOid, 1)

    override fun add(tableOid: Oid, pageid: PageId): PageId {
        val nextPageId = if (pageid == -1) {
            maxPageId
        } else {
            pageid
        }
        maxPageId = 1 + max(maxPageId, pageid)
        return pageCache.getAndPin(tableOid).use { cachedPage ->
            cachedPage.diskPage.putRecord(OidPageidRecord(intField(tableOid), intField(nextPageId)).asBytes()).let {
                if (it.isOutOfSpace) {
                    throw SystemCatalogException("Directory page overflow for relation $tableOid")
                }
            }
            nextPageId
        }
    }

}


class RootRecords(private val pageCache: PageCache,
                  private val startRootPageId: PageId = 1,
                  private val maxRootPageCount: Int = MAX_ROOT_PAGE_COUNT): Iterable<OidPageidRecord> {
    override fun iterator(): Iterator<OidPageidRecord> = RootRecordIteratorImpl(pageCache, startRootPageId, maxRootPageCount)
}


private fun <T> advanceRecord(currentPage: CachedPage?, currentRecordIdx: RecordId, recordBytesParser: Function<ByteArray, T>): Pair<T?, RecordId>? {
    var nextRecordIdx = currentRecordIdx
    do {
        nextRecordIdx++
        currentPage?.let {
            val nextRecord = it.diskPage.getRecord(nextRecordIdx)
            if (nextRecord.isOk) {
                return recordBytesParser.apply(nextRecord.bytes) to nextRecordIdx
            }
            if (nextRecord.isOutOfRange) {
                return null
            }
        } ?: return null
    } while (true)
}
