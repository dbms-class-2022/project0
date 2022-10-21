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
    "planet.id" to planetRecord().f1.first as AttributeType<Comparable<Any>>,
    "planet.name" to planetRecord().f2.first as AttributeType<Comparable<Any>>,
    "planet.distance" to planetRecord().f3.first as AttributeType<Comparable<Any>>,
    "spacecraft.id" to spacecraftRecord().f1.first as AttributeType<Comparable<Any>>,
    "spacecraft.name" to spacecraftRecord().f2.first as AttributeType<Comparable<Any>>,
    "spacecraft.capacity" to spacecraftRecord().f3.first as AttributeType<Comparable<Any>>,
    "flight.num" to flightRecord().f1.first as AttributeType<Comparable<Any>>,
    "flight.planet_id" to flightRecord().f2.first as AttributeType<Comparable<Any>>,
    "flight.spacecraft_id" to flightRecord().f3.first as AttributeType<Comparable<Any>>,
    "ticket.flight_num" to ticketRecord().f1.first as AttributeType<Comparable<Any>>,
    "ticket.pax_name" to ticketRecord().f2.first as AttributeType<Comparable<Any>>,
    "ticket.price" to ticketRecord().f3.first as AttributeType<Comparable<Any>>,
)
