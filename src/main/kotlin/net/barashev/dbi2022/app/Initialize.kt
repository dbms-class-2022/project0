package net.barashev.dbi2022.app

import net.barashev.dbi2022.*
import net.barashev.dbi2022.fake.FakeIndexFactory
import net.barashev.dbi2022.fake.FakeMergeSort
import net.barashev.dbi2022.fake.FakeNestedLoops

/**
 * Please change this code and use your own factories.
 */
fun initializeFactories(storage: Storage, cacheSize: Int): Pair<PageCache, AccessMethodManager> {
    CacheManager.factory = { storage, size -> SimplePageCacheImpl(storage, size) }

    val cache = CacheManager.factory(storage, cacheSize)
    val accessMethodManager = SimpleAccessMethodManager(cache)

    Operations.sortFactory = { accessMethodManager, pageCache -> FakeMergeSort(accessMethodManager, pageCache) }
    Operations.hashFactory = { accessMethodManager, pageCache -> TODO("Not implemented") }
    Operations.innerJoinFactory = { accessMethodManager, pageCache, joinAlgorithm ->
        when (joinAlgorithm) {
            JoinAlgorithm.NESTED_LOOPS -> FakeNestedLoops(accessMethodManager, pageCache)
            else -> TODO("Join algorithm $joinAlgorithm not implemented yet")
        }
    }
    IndexManager.indexFactory = FakeIndexFactory(accessMethodManager, cache)
    Statistics.managerFactory = { _, _ -> FakeStatisticsManager() }
    return cache to accessMethodManager
}