package net.barashev.dbi2022.app

import net.barashev.dbi2022.*
import net.datafaker.Faker
import kotlin.random.Random

class DataGenerator(
    val accessMethodManager: AccessMethodManager,
    val cache: PageCache,
    val scale: Int = 1,
    fixedRowCount: Boolean
) {
    val planetCount = if (fixedRowCount) 20 else Random.nextInt(10, 100 * scale/5)
    val spacecraftCount = if (fixedRowCount) 4 else Random.nextInt(10, 20) * scale/5
    val flightCount = if (fixedRowCount) 500 else Random.nextInt(500, 1000) * scale
    val ticketCount = flightCount * 3
    val faker = Faker()

    init {
        populatePlanets(planetCount)
        populateSpacecrafts(spacecraftCount)
        populateFlights(flightCount, planetCount, spacecraftCount)
        populateTickets(ticketCount, flightCount)
    }
}

private fun DataGenerator.populatePlanets(planetCount: Int) {
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

private fun DataGenerator.populateSpacecrafts(spacecraftCount: Int) {
    val spacecraftTable = accessMethodManager.createTable("spacecraft")
    TableBuilder(accessMethodManager, cache, spacecraftTable).use { builder ->
        (1..spacecraftCount).forEach { idx ->
            SpacecraftRecord(intField(idx), stringField(faker.pokemon().name()), intField(Random.nextInt(1, 10))).also {
                builder.insert(it.asBytes())
            }
        }
    }
}

private fun DataGenerator.populateFlights(flightCount: Int, planetCount: Int, spacecraftCount: Int) {
    val flightTable = accessMethodManager.createTable("flight")
    TableBuilder(accessMethodManager, cache, flightTable).use { builder ->
        (1..flightCount).forEach {idx ->
            FlightRecord(intField(idx), intField(Random.nextInt(planetCount)), intField(Random.nextInt(spacecraftCount))).also {
                builder.insert(it.asBytes())
            }
        }
    }
}

private fun DataGenerator.populateTickets(ticketCount: Int, flightCount: Int) {
    val ticketTable = accessMethodManager.createTable("ticket")
    TableBuilder(accessMethodManager, cache, ticketTable).use { builder ->
        (1..ticketCount).forEach { idx ->
            TicketRecord(intField(Random.nextInt(flightCount)), stringField(faker.name().fullName()), doubleField(Random.nextDouble(50.0, 200.0))).also {
                builder.insert(it.asBytes())
            }
        }
    }
}

