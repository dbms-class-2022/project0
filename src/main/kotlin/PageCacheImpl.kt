
internal data class StatsImpl(var cacheHitCount: Int = 0, var cacheMissCount: Int = 0): PageCacheStats {
    override val cacheHit: Int
        get() = cacheHitCount
    override val cacheMiss: Int
        get() = cacheMissCount
}

internal class CachedPageImpl(override val diskPage: DiskPage, private val evict: (CachedPageImpl)->Unit, var pinCount: Int = 1): CachedPage {
    override fun close() {
        assert(pinCount > 0)
        pinCount -= 1
        if (pinCount == 0) {
            evict(this)
        }
    }
}

class DummyPageCacheImpl(internal val storage: Storage, private val maxCacheSize: Int = -1): PageCache {
    internal val statsImpl = StatsImpl()
    override val stats: PageCacheStats get() = statsImpl
    internal val cache = mutableMapOf<PageId, CachedPageImpl>()

    override fun load(startPageId: PageId, pageCount: Int) = doLoad(startPageId, pageCount, this::doAddPage)


    internal fun doLoad(startPageId: PageId, pageCount: Int, addPage: (page: DiskPage) -> CachedPageImpl) {
        storage.readPageSequence(startPageId, pageCount) { diskPage ->
            if (!cache.containsKey(diskPage.id)) {
                addPage(diskPage)
            }
        }
    }

    override fun getAndPin(pageId: PageId): CachedPage = doGetAndPin(
        pageId,
        { cacheHit -> if (cacheHit) statsImpl.cacheHitCount += 1 else statsImpl.cacheMissCount += 1 },
        this::doAddPage
    )

    internal fun doGetAndPin(pageId: PageId, recordCacheHit: (Boolean) -> Unit, addPage: (page: DiskPage) -> CachedPageImpl): CachedPageImpl {
        var cacheHit = true
        return cache.getOrElse(pageId) {
            cacheHit = false
            addPage(storage.readPage(pageId))
        }.also {
            it.pinCount += 1
            recordCacheHit(cacheHit)
        }
    }

    private fun doAddPage(page: DiskPage): CachedPageImpl {
        if (cache.size == maxCacheSize) {
            evict(evictCandidate())
        }
        return CachedPageImpl(page, this::evict, 0).also {
            cache[page.id] = it
        }
    }

    override fun createSubCache(size: Int): PageCache = SubcacheImpl(this, size)

    override fun flush() {
        cache.forEach { (_, cachedPage) -> storage.writePage(cachedPage.diskPage) }
    }

    private fun evictCandidate(): CachedPageImpl {
        return cache.values.firstOrNull {
            it.pinCount == 0
        } ?: throw IllegalStateException("All pages are pinned, there is no victim for eviction")
    }

    internal fun evict(cachedPage: CachedPageImpl) {
        storage.writePage(cachedPage.diskPage)
        cache.remove(cachedPage.diskPage.id)
    }
}

class SubcacheImpl(private val mainCache: DummyPageCacheImpl, private val maxCacheSize: Int): PageCache {
    private val statsImpl = StatsImpl()
    override val stats: PageCacheStats get() = statsImpl
    private val subcachePages = mutableSetOf<PageId>()
    override fun load(startPageId: PageId, pageCount: Int) {
        mainCache.doLoad(startPageId, pageCount, this::doAddPage)
    }

    override fun getAndPin(pageId: PageId): CachedPage {
        var localCacheHit = subcachePages.contains(pageId)
        if (localCacheHit) statsImpl.cacheHitCount += 1 else statsImpl.cacheMissCount += 1
        return mainCache.doGetAndPin(
            pageId,
            { cacheHit ->
                if (cacheHit) {
                    mainCache.statsImpl.cacheHitCount += 1
                } else {
                    mainCache.statsImpl.cacheMissCount += 1
                }
            },
            this::doAddPage)
    }

    private fun doAddPage(page: DiskPage): CachedPageImpl {
        if (subcachePages.size == maxCacheSize) {
            evictCandidate().let {
                mainCache.evict(it)
                subcachePages.remove(it.diskPage.id)
            }
        }
        return CachedPageImpl(page, this::evict, 0).also {
            mainCache.cache[page.id] = it
            subcachePages.add(page.id)
        }
    }

    override fun createSubCache(size: Int): PageCache {
        TODO("Not yet implemented")
    }

    override fun flush() {
        subcachePages.mapNotNull { mainCache.cache[it] }.forEach { page -> mainCache.storage.writePage(page.diskPage) }
    }

    private fun evictCandidate(): CachedPageImpl =
        mainCache.cache[subcachePages.first()]!!

    internal fun evict(cachedPage: CachedPageImpl) {
        subcachePages.remove(cachedPage.diskPage.id)
    }


}