package net.barashev.dbi2022

import net.barashev.dbi2022.app.parseFilterClause
import net.barashev.dbi2022.app.parseJoinClause
import org.junit.jupiter.api.Test

class ParserTest {
    @Test
    fun `parse join clause`() {
        println(parseJoinClause("planet.id:flight.planet_id flight.spacecraft_id:spacecraft.id flight.num:ticket.flight_num"))
    }

    @Test
    fun `parse filter clause`() {
        println(parseFilterClause("spacecraft.capacity = 3 && ticket.price < 100"))
    }
}

