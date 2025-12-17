package com.minileaf.core.index

import com.fasterxml.jackson.databind.node.ObjectNode
import com.minileaf.core.query.QueryEngine
import com.minileaf.core.storage.Index

/**
 * Partial index that only indexes documents matching a filter expression.
 * Reduces index size and improves performance for queries on a subset of documents.
 *
 * @param ID The type of document identifier
 * @param delegate The underlying index implementation
 * @param partialFilterExpression Filter determining which documents to index
 */
class PartialIndex<ID : Any>(
    private val delegate: Index<ID>,
    private val partialFilterExpression: Map<String, Any>
) : Index<ID> by delegate {

    override fun insert(id: ID, document: ObjectNode) {
        if (matches(document)) {
            delegate.insert(id, document)
        }
    }

    override fun update(id: ID, oldDocument: ObjectNode?, newDocument: ObjectNode) {
        val oldMatches = oldDocument != null && matches(oldDocument)
        val newMatches = matches(newDocument)

        when {
            oldMatches && newMatches -> delegate.update(id, oldDocument, newDocument)
            oldMatches && !newMatches -> oldDocument?.let { delegate.remove(id, it) }
            !oldMatches && newMatches -> delegate.insert(id, newDocument)
        }
    }

    override fun remove(id: ID, document: ObjectNode) {
        if (matches(document)) {
            delegate.remove(id, document)
        }
    }

    /**
     * Returns true if the document matches the partial filter expression.
     */
    private fun matches(document: ObjectNode): Boolean {
        return QueryEngine.matches(document, partialFilterExpression)
    }

    /**
     * Returns the partial filter expression.
     */
    fun getPartialFilter(): Map<String, Any> = partialFilterExpression
}
