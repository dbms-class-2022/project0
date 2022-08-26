
internal data class StatsImpl(var cacheHitCount: Int = 0, var cacheMissCount: Int = 0): PageCacheStats {
    override val cacheHit: Int
        get() = cacheHitCount
    override val cacheMiss: Int
        get() = cacheMissCount
}

internal class CachedPageImpl(override val diskPage: DiskPage, private val dummyPageCacheImpl: DummyPageCacheImpl, var pinCount: Int = 1): CachedPage {
    override fun close() {
        assert(pinCount > 0)
        pinCount -= 1
        if (pinCount == 0) {
            dummyPageCacheImpl.evict(this)
        }
    }
}

class DummyPageCacheImpl(private val storage: Storage, private val maxCacheSize: Int = -1): PageCache {
    private val statsImpl = StatsImpl()
    override val stats: PageCacheStats get() = statsImpl
    private val cache = mutableMapOf<PageId, CachedPageImpl>()

    override fun load(startPageId: PageId, pageCount: Int) {
        storage.readPageSequence(startPageId, pageCount) { diskPage ->
            if (!cache.containsKey(diskPage.id)) {
                addPage(diskPage)
            }
        }
    }

    override fun getAndPin(pageId: PageId): CachedPage =
        cache.getOrElse(pageId) {
            addPage(storage.readPage(pageId))
        }.also { it.pinCount += 1 }

    private fun addPage(page: DiskPage): CachedPageImpl {
        if (cache.size == maxCacheSize) {
            evict(evictCandidate())
        }
        return CachedPageImpl(page, this, 0).also {
            cache[page.id] = it
        }
    }

    override fun createSubCache(size: Int): PageCache {
        TODO("Not yet implemented")
    }

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