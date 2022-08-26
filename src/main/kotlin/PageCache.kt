interface CachedPage : AutoCloseable {
    val diskPage: DiskPage
}

interface PageCache {
    fun load(startPageId: PageId, pageCount: Int = 1)
    fun getAndPin(pageId: PageId): CachedPage
    fun createSubCache(size: Int): PageCache
    fun flush()
    val stats: PageCacheStats
}

interface PageCacheStats {
    val cacheHit: Int
    val cacheMiss: Int
}