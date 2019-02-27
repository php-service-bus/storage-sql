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
use Latitude\QueryBuilder\Query\DeleteQuery;
use Latitude\QueryBuilder\Query\InsertQuery;
use Latitude\QueryBuilder\Query\SelectQuery;
use Latitude\QueryBuilder\Query\UpdateQuery;
use Latitude\QueryBuilder\QueryFactory;
use ServiceBus\Storage\Common\Exceptions\IncorrectParameterCast;
use ServiceBus\Storage\Common\Exceptions\OneResultExpected;
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
 * @return SelectQuery
 */
function selectQuery(string $fromTable, string ...$columns): SelectQuery
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
 * @return UpdateQuery
 */
function updateQuery(string $tableName, $toUpdate): UpdateQuery
{
    $values = true === \is_object($toUpdate) ? castObjectToArray($toUpdate) : $toUpdate;

    return queryBuilder()->update($tableName, $values);
}

/**
 * Create delete query (for PostgreSQL).
 *
 * @param string $fromTable
 *
 * @return DeleteQuery
 */
function deleteQuery(string $fromTable): DeleteQuery
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
 * @return InsertQuery
 */
function insertQuery(string $toTable, $toInsert): InsertQuery
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
