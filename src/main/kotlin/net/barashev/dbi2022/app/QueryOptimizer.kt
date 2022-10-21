package net.barashev.dbi2022.app

import net.barashev.dbi2022.AccessMethodManager
import net.barashev.dbi2022.PageCache

interface QueryOptimizer {
    fun buildPlan(innerJoins: JoinTree, filters: List<FilterSpec>): QueryPlan
}

object Optimizer {
    var factory: (AccessMethodManager, PageCache) -> QueryOptimizer = { _, _ ->
        FakeOptimizer()
    }
}

class FakeOptimizer : QueryOptimizer {
    override fun buildPlan(innerJoins: JoinTree, filters: List<FilterSpec>): QueryPlan = QueryPlan(innerJoins, filters)
}
