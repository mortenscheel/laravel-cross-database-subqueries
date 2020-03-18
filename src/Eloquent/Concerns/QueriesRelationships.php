<?php

namespace Hoyvoy\CrossDatabase\Eloquent\Concerns;

use Closure;
use Hoyvoy\CrossDatabase\CanCrossDatabaseShazaamInterface;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\MorphTo;
use Illuminate\Database\Eloquent\Relations\Relation;
use Illuminate\Support\Arr;
use Illuminate\Support\Str;
use RuntimeException;

trait QueriesRelationships
{

    /**
     * Add a relationship count / exists condition to the query.
     *
     * @param \Illuminate\Database\Eloquent\Relations\Relation|string $relation
     * @param string $operator
     * @param int $count
     * @param string $boolean
     * @param \Closure|null $callback
     * @return \Illuminate\Database\Eloquent\Builder|\Illuminate\Database\Eloquent\Concerns\QueriesRelationships
     *
     * @throws \RuntimeException
     */
    public function has($relation, $operator = '>=', $count = 1, $boolean = 'and', Closure $callback = null)
    {
        if (is_string($relation)) {
            if (strpos($relation, '.') !== false) {
                return $this->hasNested($relation, $operator, $count, $boolean, $callback);
            }

            $relation = $this->getRelationWithoutConstraints($relation);
        }

        if ($relation instanceof MorphTo) {
            throw new RuntimeException('Please use whereHasMorph() for MorphTo relationships.');
        }


        // If we only need to check for the existence of the relation, then we can optimize
        // the subquery to only run a "where exists" clause instead of this full "count"
        // clause. This will make these queries run much faster compared with a count.
        $method = $this->canUseExistsForExistenceCheck($operator, $count)
            ? 'getRelationExistenceQuery'
            : 'getRelationExistenceCountQuery';

        $hasQuery = $relation->{$method}(
            $relation->getRelated()->newQueryWithoutRelationships(), $this
        );


        // Next we will call any given callback as an "anonymous" scope so they can get the
        // proper logical grouping of the where clauses if needed by this Eloquent query
        // builder. Then, we will be ready to finalize and return this query instance.
        if ($callback) {
            $hasQuery->callScope($callback);
        }

        if (!$this->isSameHost($relation->getConnection())) {
            // Subquery is not possible. Do a workaround in stead:
            if ($relation instanceof BelongsTo) {
                // 1. Execute a separate query to obtain ids of related models.
                // 2. Use these ids in a standard where clause on the foreign key.
                // First modify hasQuery and execute it to get the owner ids.
                /** @var \Illuminate\Database\Query\Builder $remoteQuery */
                $remoteQuery = $hasQuery->getQuery();
                // Remove the where clause joining the two tables
                $linkClause = [
                    'type' => 'Column',
                    'first' => $relation->getQualifiedForeignKeyName(),
                    'operator' => '=',
                    'second' => $relation->getQualifiedOwnerKeyName(),
                    'boolean' => 'and'
                ];
                foreach ($remoteQuery->wheres as $i => $where) {
                    if ($where === $linkClause) {
                        unset($remoteQuery->wheres[$i]);
                        break;
                    }
                }
                // Obtain matching owner ids
                $ids = $remoteQuery->pluck('id');
                // Return the constrained query
                if ($operator === '<') {
                    return $this->where(function ($clause) use ($relation, $ids) {
                        $clause->whereNotIn($relation->getForeignKeyName(), $ids);
                        $clause->orWhereNull($relation->getForeignKeyName());
                    });
                }
                return $this->whereIn($relation->getForeignKeyName(), $ids);
            }
        }

        return $this->addHasWhere(
            $hasQuery, $relation, $operator, $count, $boolean
        );
    }

    /**
     * @return bool
     */
    protected function isSameHost(ConnectionInterface $connection)
    {
        $ownConfig = Arr::only($this->getConnection()->getConfig(), [
            'driver',
            'host',
            'port'
        ]);
        $otherConfig = Arr::only($connection->getConfig(), [
            'driver',
            'host',
            'port'
        ]);
        return $ownConfig === $otherConfig;
    }

    /**
     * Add the "has" condition where clause to the query.
     *
     * @param \Illuminate\Database\Eloquent\Builder $hasQuery
     * @param \Illuminate\Database\Eloquent\Relations\Relation $relation
     * @param string $operator
     * @param int $count
     * @param string $boolean
     *
     * @return \Illuminate\Database\Eloquent\Builder|static
     */
    protected function addHasWhere(Builder $hasQuery, Relation $relation, $operator, $count, $boolean)
    {
        // If connection implements CanCrossDatabaseShazaamInterface we must attach database
        // connection name in from to be used by grammar when query compiled
        if ($this->getConnection() instanceof CanCrossDatabaseShazaamInterface) {
            $subqueryConnection = $hasQuery->getConnection()->getDatabaseName();
            $queryConnection = $this->getConnection()->getDatabaseName();
            if ($queryConnection !== $subqueryConnection) {
                $queryFrom = $hasQuery->getConnection()->getTablePrefix() . '<-->' . $hasQuery->getQuery()->from . '<-->' . $subqueryConnection;
                $hasQuery->from($queryFrom);
            }
        }

        return parent::addHasWhere($hasQuery, $relation, $operator, $count, $boolean);
    }

    /**
     * Add subselect queries to count the relations.
     *
     * @param mixed $relations
     *
     * @return $this
     */
    public function withCount($relations)
    {
        if (empty($relations)) {
            return $this;
        }

        if (is_null($this->query->columns)) {
            $this->query->select([$this->query->from . '.*']);
        }

        $relations = is_array($relations) ? $relations : func_get_args();

        foreach ($this->parseWithRelations($relations) as $name => $constraints) {
            // First we will determine if the name has been aliased using an "as" clause on the name
            // and if it has we will extract the actual relationship name and the desired name of
            // the resulting column. This allows multiple counts on the same relationship name.
            $segments = explode(' ', $name);

            unset($alias);

            if (count($segments) == 3 && Str::lower($segments[1]) == 'as') {
                list($name, $alias) = [$segments[0], $segments[2]];
            }

            $relation = $this->getRelationWithoutConstraints($name);

            // Here we will get the relationship count query and prepare to add it to the main query
            // as a sub-select. First, we'll get the "has" query and use that to get the relation
            // count query. We will normalize the relation name then append _count as the name.
            $query = $relation->getRelationExistenceCountQuery(
                $relation->getRelated()->newQuery(), $this
            );

            $query->callScope($constraints);

            // If connection implements CanCrossDatabaseShazaamInterface we must attach database
            // connection name in from to be used by grammar when query compiled
            if ($this->getConnection() instanceof CanCrossDatabaseShazaamInterface) {
                $subqueryConnection = $query->getConnection()->getDatabaseName();
                $queryConnection = $this->getConnection()->getDatabaseName();
                if ($queryConnection != $subqueryConnection) {
                    $queryFrom = $query->getConnection()->getTablePrefix() . '<-->' . $query->getQuery()->from . '<-->' . $subqueryConnection;
                    $query->from($queryFrom);
                }
            }

            $query = $query->mergeConstraintsFrom($relation->getQuery())->toBase();

            if (count($query->columns) > 1) {
                $query->columns = [$query->columns[0]];
            }

            // Finally we will add the proper result column alias to the query and run the subselect
            // statement against the query builder. Then we will return the builder instance back
            // to the developer for further constraint chaining that needs to take place on it.
            $column = $alias ?? Str::snake($name . '_count');

            $this->selectSub($query, $column);
        }

        return $this;
    }

    protected function generateCrossHostQuery(Relation $relation)
    {

    }
}
