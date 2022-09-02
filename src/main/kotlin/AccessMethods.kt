import java.util.function.Function

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
        currentPage?.close()
        while (rootRecords.hasNext()) {
            val nextOidPageidRecord = rootRecords.next()
            if (nextOidPageidRecord.value1 == tableOid) {
                return pageCache.getAndPin(nextOidPageidRecord.value2)
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
