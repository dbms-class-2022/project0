package net.barashev.dbi2022.app

import net.barashev.dbi2022.AccessMethodManager
import net.barashev.dbi2022.PageCache

/**
 * Query optimizer transforms an initial query plan into equivalent plan which is likely to be more efficient.
 * The input is a query plan which consists of a left-recursive join tree and a list of filters.
 * Technically a join tree is a list of join pairs, and the order of join pairs in the list corresponds
 * to the order in which they will be joined. Thus, one of the optimizer tasks is to find an optimal permutation of the
 * join pairs.
 * Filters will be applied to the whole join result, if specified directly in QueryPlan::filters property.
 * They can also be pushed down into joins using JoinSpec::filter property.
 * Thus, another optimizer task is to decide whether some filter needs to be pushed down or not.
 *
 * Technical subtleties:
 * - except for the first pair in the join list, the left component of each pair must refer to one of the tables already
 *   used in the list prefix.
 *   For instance, this plan is ok: planet.id:flight.planet_id flight.spacecraft_id:spacecraft.id
 *   This plan, where the second pair operands are swapped,  is not ok: planet.id:flight.planet_id spacecraft.id:flight.spacecraft_id
 * A query executor materializes all intermediate results (that is, writes them to the "disk storage")
 */
interface QueryOptimizer {
    fun buildPlan(initialPlan: QueryPlan): QueryPlan
}

object Optimizer {
    var factory: (AccessMethodManager, PageCache) -> QueryOptimizer = { _, _ ->
        WorthlessOptimizer()
    }
}

class WorthlessOptimizer : QueryOptimizer {
    override fun buildPlan(initialPlan: QueryPlan): QueryPlan = initialPlan
}
