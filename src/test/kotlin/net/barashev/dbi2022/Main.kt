package net.barashev.dbi2022

import net.datafaker.Faker
import org.junit.jupiter.api.Test
import java.util.function.Function
import kotlin.random.Random

class Main {
    @Test
    fun demo() {
        DatabaseEngine(createHardDriveEmulatorStorage(), 100).apply {
            populatePlanets()
            populateSpacecrafts()
            val joinResult = JoinExecutor(accessMethodManager, cache, tableRecordParsers, attributeValueParsers).run {
                join(listOf(JoinSpec("planet", "planet.id"), JoinSpec("spacecraft", "spacecraft.id")))
            }
            val joinedTables = joinResult.first.split(",")
            accessMethodManager.createFullScan(joinResult.first) { bytes ->
                parseJoinedRecord(bytes, joinedTables, tableRecordParsers)
            }.forEach {
                it.entries.forEach { (tableName, recordBytes) ->
                    println("$tableName: ${tableRecordParsers[tableName]!!.invoke(recordBytes)}")
                }
                println("----")
            }
        }
    }
}

class DatabaseEngine(val storage: Storage, val cacheSize: Int) {
    internal val accessMethodManager: AccessMethodManager
    internal val cache: PageCache

    init {
        val (cache, accessMethodManager) = initializeFactories(storage, cacheSize)
        this.cache = cache
        this.accessMethodManager = accessMethodManager
    }
}

val faker = Faker()
typealias PlanetRecord = Record3<Int, String, Double>
fun planetRecord(bytes: ByteArray): PlanetRecord = PlanetRecord(intField(), stringField(), doubleField()).fromBytes(bytes)

typealias SpacecraftRecord = Record3<Int, String, Int>
fun spacecraftRecord(bytes: ByteArray) = SpacecraftRecord(intField(), stringField(), intField()).fromBytes(bytes)

typealias FlightRecord = Record3<Int, Int, Int>
fun flightRecord(bytes: ByteArray) = FlightRecord(intField(), intField(), intField()).fromBytes(bytes)

typealias TicketRecord = Record3<Int, String, Double>

val tableRecordParsers = mapOf<String, TableRecordParser>(
    "planet" to { planetRecord(it) as Record3<Any, Any, Any> },
    "spacecraft" to  { spacecraftRecord(it) as Record3<Any, Any, Any> },
)

val attributeValueParsers = mapOf<String, Function<ByteArray, Comparable<Any>>>(
    "planet.id" to Function { planetRecord(it).value1 as Comparable<Any> },
    "planet.name" to Function { planetRecord(it).value2 as Comparable<Any> },
    "planet.distance" to Function { planetRecord(it).value3 as Comparable<Any> },
    "spacecraft.id" to Function { spacecraftRecord(it).value1 as Comparable<Any> },
    "spacecraft.name" to Function { spacecraftRecord(it).value2 as Comparable<Any> },
    "spacecraft.capacity" to Function { spacecraftRecord(it).value3 as Comparable<Any> },
)
private fun DatabaseEngine.populatePlanets(planetCount: Int = Random.nextInt(100)) {
    val planetTable = accessMethodManager.createTable("planet")
    TableBuilder(accessMethodManager, cache, planetTable).use { insert ->
        (1..planetCount).forEach {idx ->
            PlanetRecord(intField(idx), stringField(faker.starCraft().planet()), doubleField(Random.nextDouble(100.0, 900.0))).also {
                insert.insert(it.asBytes())
            }
        }
    }
    Statistics.managerFactory(accessMethodManager, cache).buildStatistics(
        "planet",
        "distance",
        doubleField().first,
        10
    ) { planetRecord(it).component3() }
}

private fun DatabaseEngine.populateSpacecrafts(spacecraftCount: Int = Random.nextInt(10, 20)) {
    val spacecraftTable = accessMethodManager.createTable("spacecraft")
    TableBuilder(accessMethodManager, cache, spacecraftTable).use { builder ->
        (1..spacecraftCount).forEach { idx ->
            SpacecraftRecord(intField(idx), stringField(faker.space().company()), intField(Random.nextInt(1, 10))).also {
                builder.insert(it.asBytes())
            }
        }
    }
}


private fun initializeFactories(storage: Storage, cacheSize: Int): Pair<PageCache, AccessMethodManager> {
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