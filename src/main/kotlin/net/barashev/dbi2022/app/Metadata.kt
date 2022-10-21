package net.barashev.dbi2022.app

import java.util.function.Function

import net.barashev.dbi2022.Record3
import net.barashev.dbi2022.doubleField
import net.barashev.dbi2022.intField
import net.barashev.dbi2022.stringField

typealias PlanetRecord = Record3<Int, String, Double>
fun planetRecord(bytes: ByteArray): PlanetRecord = PlanetRecord(intField(), stringField(), doubleField()).fromBytes(bytes)

typealias SpacecraftRecord = Record3<Int, String, Int>
fun spacecraftRecord(bytes: ByteArray) = SpacecraftRecord(intField(), stringField(), intField()).fromBytes(bytes)

typealias FlightRecord = Record3<Int, Int, Int>
fun flightRecord(bytes: ByteArray) = FlightRecord(intField(), intField(), intField()).fromBytes(bytes)

typealias TicketRecord = Record3<Int, String, Double>
fun ticketRecord(bytes: ByteArray) = TicketRecord(intField(), stringField(), doubleField()).fromBytes(bytes)

val tableRecordParsers = mapOf<String, TableRecordParser>(
    "planet" to { planetRecord(it) as Record3<Any, Any, Any> },
    "spacecraft" to  { spacecraftRecord(it) as Record3<Any, Any, Any> },
    "flight" to { flightRecord(it) as Record3<Any, Any, Any> },
    "ticket" to { ticketRecord(it) as Record3<Any, Any, Any> }
)

val attributeValueParsers = mapOf<String, Function<ByteArray, Comparable<Any>>>(
    "planet.id" to Function { planetRecord(it).value1 as Comparable<Any> },
    "planet.name" to Function { planetRecord(it).value2 as Comparable<Any> },
    "planet.distance" to Function { planetRecord(it).value3 as Comparable<Any> },
    "spacecraft.id" to Function { spacecraftRecord(it).value1 as Comparable<Any> },
    "spacecraft.name" to Function { spacecraftRecord(it).value2 as Comparable<Any> },
    "spacecraft.capacity" to Function { spacecraftRecord(it).value3 as Comparable<Any> },
    "flight.num" to Function { flightRecord(it).value1 as Comparable<Any>},
    "flight.planet_id" to Function { flightRecord(it).value2 as Comparable<Any>},
    "flight.spacecraft_id" to Function { flightRecord(it).value3 as Comparable<Any>},
    "ticket.flight_num" to Function { ticketRecord(it).value1 as Comparable<Any>},
    "ticket.pax_name" to Function { ticketRecord(it).value2 as Comparable<Any>},
    "ticket.price" to Function { ticketRecord(it).value3 as Comparable<Any>},
)
