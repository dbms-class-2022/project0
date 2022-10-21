package net.barashev.dbi2022.app

import net.barashev.dbi2022.*
import java.util.function.Function

typealias PlanetRecord = Record3<Int, String, Double>

fun planetRecord(bytes: ByteArray? = null): PlanetRecord = PlanetRecord(intField(), stringField(), doubleField()).let {rec ->
    bytes?.let{ rec.fromBytes(it) } ?: rec
}

typealias SpacecraftRecord = Record3<Int, String, Int>
fun spacecraftRecord(bytes: ByteArray? = null) = SpacecraftRecord(intField(), stringField(), intField()).let {rec ->
    bytes?.let{ rec.fromBytes(it) } ?: rec
}

typealias FlightRecord = Record3<Int, Int, Int>
fun flightRecord(bytes: ByteArray? = null) = FlightRecord(intField(), intField(), intField()).let {rec ->
    bytes?.let{ rec.fromBytes(it) } ?: rec
}

typealias TicketRecord = Record3<Int, String, Double>
fun ticketRecord(bytes: ByteArray? = null) = TicketRecord(intField(), stringField(), doubleField()).let {rec ->
    bytes?.let{ rec.fromBytes(it) } ?: rec
}

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

val attributeTypes = mapOf(
    "planet.id" to planetRecord().type1,
    "planet.name" to planetRecord().type2,
    "planet.distance" to planetRecord().type3,
    "spacecraft.id" to spacecraftRecord().type1 ,
    "spacecraft.name" to spacecraftRecord().type2,
    "spacecraft.capacity" to spacecraftRecord().type3,
    "flight.num" to flightRecord().type1,
    "flight.planet_id" to flightRecord().type2,
    "flight.spacecraft_id" to flightRecord().type3,
    "ticket.flight_num" to ticketRecord().type1,
    "ticket.pax_name" to ticketRecord().type2,
    "ticket.price" to ticketRecord().type3,
)
