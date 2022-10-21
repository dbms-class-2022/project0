package net.barashev.dbi2022

class TableBuilder(private val accessMethodManager: AccessMethodManager, private val cache: PageCache, private val tableOid: Oid): AutoCloseable {
    private var currentPage: CachedPage = newPage()

    private fun newPage(): CachedPage {
        currentPage?.close()
        return cache.getAndPin(accessMethodManager.addPage(tableOid))
    }

    fun insert(record: ByteArray) {
        currentPage.putRecord(record).let {
            if (it.isOutOfSpace) {
                currentPage = newPage()
                assert(currentPage.putRecord(record).isOk)
            }
        }
    }

    override fun close() {
        currentPage.close()
    }
}
