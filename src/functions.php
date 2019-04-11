<?php

/**
 * SQL databases adapters implementation.
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Storage\Sql;

use function Amp\call;
use function Latitude\QueryBuilder\field;
use Amp\Promise;
use Latitude\QueryBuilder\CriteriaInterface;
use Latitude\QueryBuilder\Engine\PostgresEngine;
use Latitude\QueryBuilder\EngineInterface;
use Latitude\QueryBuilder\Query as LatitudeQuery;
use Latitude\QueryBuilder\QueryFactory;
use ServiceBus\Storage\Common\BinaryDataDecoder;
use ServiceBus\Storage\Common\Exceptions\IncorrectParameterCast;
use ServiceBus\Storage\Common\Exceptions\OneResultExpected;
use ServiceBus\Storage\Common\QueryExecutor;
use ServiceBus\Storage\Common\ResultSet;

/**
 * Collect iterator data
 * Not recommended for use on large amounts of data.
 *
 * @noinspection   PhpDocRedundantThrowsInspection
 * @psalm-suppress MixedTypeCoercion Incorrect resolving the value of the promise
 *
 * @param ResultSet $iterator
 *
 * @throws \ServiceBus\Storage\Common\Exceptions\ResultSetIterationFailed
 *
 * @return Promise<array<int, mixed>|null>
 */
function fetchAll(ResultSet $iterator): Promise
{
    /** @psalm-suppress InvalidArgument */
    return call(
    /** @psalm-suppress InvalidReturnType Incorrect resolving the value of the generator */
        static function(ResultSet $iterator): \Generator
        {
            $array = [];

            /** @psalm-suppress TooManyTemplateParams Wrong Promise template */
            while (yield $iterator->advance())
            {
                $array[] = $iterator->getCurrent();
            }

            return $array;
        },
        $iterator
    );
}

/**
 * Extract 1 result.
 *
 * @noinspection   PhpDocRedundantThrowsInspection
 * @psalm-suppress MixedTypeCoercion Incorrect resolving the value of the promise
 *
 * @param ResultSet $iterator
 *
 * @throws \ServiceBus\Storage\Common\Exceptions\ResultSetIterationFailed
 * @throws \ServiceBus\Storage\Common\Exceptions\OneResultExpected The result must contain only 1 row
 *
 * @return Promise<array<string, mixed>|null>
 */
function fetchOne(ResultSet $iterator): Promise
{
    /** @psalm-suppress InvalidArgument */
    return call(
        static function(ResultSet $iterator): \Generator
        {
            /**
             * @psalm-suppress TooManyTemplateParams Wrong Promise template
             *
             * @var array $collection
             */
            $collection   = yield fetchAll($iterator);
            $resultsCount = \count($collection);

            if (0 === $resultsCount || 1 === $resultsCount)
            {
                /** @var array|bool $endElement */
                $endElement = \end($collection);

                return false !== $endElement ? $endElement : null;
            }

            throw new OneResultExpected(
                \sprintf(
                    'A single record was requested, but the result of the query execution contains several ("%d")',
                    $resultsCount
                )
            );
        },
        $iterator
    );
}

/**
 * Create & execute SELECT query.
 *
 * @psalm-param    array<mixed, \Latitude\QueryBuilder\CriteriaInterface> $criteria
 * @psalm-param    array<string, string> $orderBy
 * @psalm-suppress MixedTypeCoercion
 *
 * @param QueryExecutor                              $queryExecutor
 * @param string                                     $tableName
 * @param \Latitude\QueryBuilder\CriteriaInterface[] $criteria
 * @param int|null                                   $limit
 * @param array                                      $orderBy
 *
 * @throws \ServiceBus\Storage\Common\Exceptions\ConnectionFailed Could not connect to database
 * @throws \ServiceBus\Storage\Common\Exceptions\InvalidConfigurationOptions
 * @throws \ServiceBus\Storage\Common\Exceptions\StorageInteractingFailed Basic type of interaction errors
 * @throws \ServiceBus\Storage\Common\Exceptions\UniqueConstraintViolationCheckFailed
 *
 * @return Promise<\ServiceBus\Storage\Common\ResultSet>
 */
function find(QueryExecutor $queryExecutor, string $tableName, array $criteria = [], ?int $limit = null, array $orderBy = []): Promise
{
    /**
     * @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args)
     * @psalm-suppress MixedArgument
     */
    return call(
        static function(string $tableName, array $criteria, ?int $limit, array $orderBy) use ($queryExecutor): \Generator
        {
            /**
             * @var string $query
             * @var array  $parameters
             * @psalm-var array<string, string|int|float|null> $parameters
             */
            [$query, $parameters] = buildQuery(selectQuery($tableName), $criteria, $orderBy, $limit);

            /**
             * @psalm-suppress TooManyTemplateParams Wrong Promise template
             * @psalm-suppress MixedTypeCoercion Invalid params() docblock
             */
            return yield $queryExecutor->execute($query, $parameters);
        },
        $tableName,
        $criteria,
        $limit,
        $orderBy
    );
}

/**
 * Create & execute DELETE query.
 *
 * @psalm-param array<mixed, \Latitude\QueryBuilder\CriteriaInterface> $criteria
 * @psalm-suppress MixedTypeCoercion
 *
 * @param QueryExecutor                              $queryExecutor
 * @param string                                     $tableName
 * @param \Latitude\QueryBuilder\CriteriaInterface[] $criteria
 *
 * @throws \ServiceBus\Storage\Common\Exceptions\ConnectionFailed Could not connect to database
 * @throws \ServiceBus\Storage\Common\Exceptions\InvalidConfigurationOptions
 * @throws \ServiceBus\Storage\Common\Exceptions\StorageInteractingFailed Basic type of interaction errors
 * @throws \ServiceBus\Storage\Common\Exceptions\UniqueConstraintViolationCheckFailed
 * @throws \ServiceBus\Storage\Common\Exceptions\ResultSetIterationFailed
 *
 * @return Promise<int>
 */
function remove(QueryExecutor $queryExecutor, string $tableName, array $criteria = []): Promise
{
    /**
     * @psalm-suppress InvalidArgument Incorrect psalm unpack parameters (...$args)
     * @psalm-suppress MixedArgument
     */
    return call(
        static function(string $tableName, array $criteria) use ($queryExecutor): \Generator
        {
            /**
             * @var string $query
             * @var array  $parameters
             * @psalm-var array<string, string|int|float|null> $parameters
             */
            [$query, $parameters] = buildQuery(deleteQuery($tableName), $criteria);

            /**
             * @psalm-suppress TooManyTemplateParams Wrong Promise template
             * @psalm-suppress MixedTypeCoercion Invalid params() docblock
             *
             * @var \ServiceBus\Storage\Common\ResultSet $resultSet
             */
            $resultSet = yield $queryExecutor->execute($query, $parameters);

            $affectedRows = $resultSet->affectedRows();

            unset($resultSet);

            return $affectedRows;
        },
        $tableName,
        $criteria
    );
}

/**
 * Create query from specified parameters.
 *
 * @psalm-param array<mixed, \Latitude\QueryBuilder\CriteriaInterface> $criteria
 * @psalm-param array<string, string>                                  $orderBy
 *
 * @param LatitudeQuery\AbstractQuery                $queryBuilder
 * @param \Latitude\QueryBuilder\CriteriaInterface[] $criteria
 * @param array                                      $orderBy
 * @param int|null                                   $limit
 *
 * @return array 0 - SQL query; 1 - query parameters
 */
function buildQuery(
    LatitudeQuery\AbstractQuery $queryBuilder,
    array $criteria = [],
    array $orderBy = [],
    ?int $limit = null
): array {
    /** @var LatitudeQuery\DeleteQuery|LatitudeQuery\SelectQuery|LatitudeQuery\UpdateQuery $queryBuilder */
    $isFirstCondition = true;

    /** @var \Latitude\QueryBuilder\CriteriaInterface $criteriaItem */
    foreach ($criteria as $criteriaItem)
    {
        $methodName = true === $isFirstCondition ? 'where' : 'andWhere';
        $queryBuilder->{$methodName}($criteriaItem);
        $isFirstCondition = false;
    }

    if ($queryBuilder instanceof LatitudeQuery\SelectQuery)
    {
        foreach ($orderBy as $column => $direction)
        {
            $queryBuilder->orderBy($column, $direction);
        }

        if (null !== $limit)
        {
            $queryBuilder->limit($limit);
        }
    }

    $compiledQuery = $queryBuilder->compile();

    return [
        $compiledQuery->sql(),
        $compiledQuery->params(),
    ];
}

/**
 * Unescape binary data.
 *
 * @psalm-param  array<string, string|int|null|float>|string $data
 *
 * @psalm-return array<string, string|int|null|float>|string
 *
 * @param QueryExecutor $queryExecutor
 * @param array|string  $data
 *
 * @return array|string
 */
function unescapeBinary(QueryExecutor $queryExecutor, $data)
{
    if ($queryExecutor instanceof BinaryDataDecoder)
    {
        if (false === \is_array($data))
        {
            return $queryExecutor->unescapeBinary((string) $data);
        }

        foreach ($data as $key => $value)
        {
            if (false === empty($value) && true === \is_string($value))
            {
                $data[$key] = $queryExecutor->unescapeBinary($value);
            }
        }
    }

    return $data;
}

/**
 * Create equals criteria.
 *
 * @param string                  $field
 * @param float|int|object|string $value
 *
 * @throws \ServiceBus\Storage\Common\Exceptions\IncorrectParameterCast
 *
 * @return \Latitude\QueryBuilder\CriteriaInterface
 */
function equalsCriteria(string $field, $value): CriteriaInterface
{
    if (true === \is_object($value))
    {
        $value = castObjectToString($value);
    }

    return field($field)->eq($value);
}

/**
 * Create not equals criteria.
 *
 * @param string                  $field
 * @param float|int|object|string $value
 *
 * @throws \ServiceBus\Storage\Common\Exceptions\IncorrectParameterCast
 *
 * @return \Latitude\QueryBuilder\CriteriaInterface
 */
function notEqualsCriteria(string $field, $value): CriteriaInterface
{
    if (true === \is_object($value))
    {
        $value = castObjectToString($value);
    }

    return field($field)->notEq($value);
}

/**
 * Create query builder.
 *
 * @param EngineInterface|null $engine
 *
 * @return QueryFactory
 */
function queryBuilder(EngineInterface $engine = null): QueryFactory
{
    return new QueryFactory($engine ?? new PostgresEngine());
}

/**
 * Create select query (for PostgreSQL).
 *
 * @noinspection PhpDocSignatureInspection
 *
 * @param string $fromTable
 * @param string ...$columns
 *
 * @return LatitudeQuery\SelectQuery
 */
function selectQuery(string $fromTable, string ...$columns): LatitudeQuery\SelectQuery
{
    return queryBuilder()->select(...$columns)->from($fromTable);
}

/**
 * Create update query (for PostgreSQL).
 *
 * @psalm-param array<string, mixed>|object $toUpdate
 *
 * @param string       $tableName
 * @param array|object $toUpdate
 *
 * @throws \ServiceBus\Storage\Common\Exceptions\IncorrectParameterCast
 *
 * @return LatitudeQuery\UpdateQuery
 */
function updateQuery(string $tableName, $toUpdate): LatitudeQuery\UpdateQuery
{
    $values = true === \is_object($toUpdate) ? castObjectToArray($toUpdate) : $toUpdate;

    return queryBuilder()->update($tableName, $values);
}

/**
 * Create delete query (for PostgreSQL).
 *
 * @param string $fromTable
 *
 * @return LatitudeQuery\DeleteQuery
 */
function deleteQuery(string $fromTable): LatitudeQuery\DeleteQuery
{
    return queryBuilder()->delete($fromTable);
}

/**
 * Create insert query (for PostgreSQL).
 *
 * @psalm-param array<string, mixed>|object $toInsert
 *
 * @param string       $toTable
 * @param array|object $toInsert
 *
 * @throws \ServiceBus\Storage\Common\Exceptions\IncorrectParameterCast
 *
 * @return LatitudeQuery\InsertQuery
 */
function insertQuery(string $toTable, $toInsert): LatitudeQuery\InsertQuery
{
    $rows = true === \is_object($toInsert) ? castObjectToArray($toInsert) : $toInsert;

    return queryBuilder()->insert($toTable, $rows);
}

/**
 * Receive object as array (property/value).
 *
 * @internal
 *
 * @psalm-return array<string, float|int|string|null>
 *
 * @param object $object
 *
 * @throws \ServiceBus\Storage\Common\Exceptions\IncorrectParameterCast
 *
 * @return array
 */
function castObjectToArray(object $object): array
{
    $result = [];

    /** @var float|int|object|string|null $value */
    foreach (getObjectVars($object) as $key => $value)
    {
        $result[toSnakeCase($key)] = cast($key, $value);
    }

    return $result;
}

/**
 * Gets the properties of the given object.
 *
 * @internal
 *
 * @psalm-return array<string, float|int|object|string|null>
 *
 * @param object $object
 *
 * @return array
 */
function getObjectVars(object $object): array
{
    $closure = \Closure::bind(
        function(): array
        {
            /**
             * @var object $this
             *
             * @psalm-suppress InvalidScope Closure:bind not supports
             */
            return \get_object_vars($this);
        },
        $object,
        $object
    );

    /**
     * @psalm-var array<string, float|int|object|string|null> $vars
     *
     * @var array $vars
     */
    $vars = $closure();

    return $vars;
}

/**
 * @internal
 *
 * Convert string from lowerCamelCase to snake_case
 *
 * @param string $string
 *
 * @return string
 */
function toSnakeCase(string $string): string
{
    $replaced = \preg_replace('/(?<!^)[A-Z]/', '_$0', $string);

    if (true === \is_string($replaced))
    {
        return \strtolower($replaced);
    }

    return $string;
}

/**
 * @internal
 *
 * @param string                       $key
 * @param float|int|object|string|null $value
 *
 * @throws \ServiceBus\Storage\Common\Exceptions\IncorrectParameterCast
 *
 * @return float|int|string|null
 *
 */
function cast(string $key, $value)
{
    if (null === $value || true === \is_scalar($value))
    {
        return $value;
    }

    /** @psalm-suppress RedundantConditionGivenDocblockType */
    if (true === \is_object($value))
    {
        return castObjectToString($value);
    }

    throw new IncorrectParameterCast(
        \sprintf(
            'The "%s" property must contain a scalar value. "%s" given',
            $key,
            \gettype($value)
        )
    );
}

/**
 * Cast object to string.
 *
 * @internal
 *
 * @param object $object
 *
 * @throws \ServiceBus\Storage\Common\Exceptions\IncorrectParameterCast
 *
 * @return string
 *
 */
function castObjectToString(object $object): string
{
    if (true === \method_exists($object, '__toString'))
    {
        /** @psalm-suppress InvalidCast Object have __toString method */
        return (string) $object;
    }

    throw new IncorrectParameterCast(
        \sprintf('"%s" must implements "__toString" method', \get_class($object))
    );
}
