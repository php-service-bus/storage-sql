<?php

/**
 * PHP Service Bus (publish-subscribe pattern implementation) storage sql implementation
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Storage\Sql\AmpPosgreSQL;

use Amp\Postgres\QueryExecutionError;
use Amp\Sql\ConnectionException;
use ServiceBus\Storage\Common\Exceptions as InternalExceptions;
use ServiceBus\Storage\Common\StorageConfiguration;

/**
 * Convert AmPHP exceptions
 *
 * @internal
 *
 * @param \Throwable $throwable
 *
 * @return InternalExceptions\UniqueConstraintViolationCheckFailed|InternalExceptions\ConnectionFailed|InternalExceptions\StorageInteractingFailed
 */
function adaptAmpThrowable(\Throwable $throwable): \Throwable
{
    if(
        $throwable instanceof QueryExecutionError &&
        true === \in_array((int) $throwable->getDiagnostics()['sqlstate'], [23503, 23505], true)
    )
    {
        return new InternalExceptions\UniqueConstraintViolationCheckFailed(
            $throwable->getMessage(),
            (int) $throwable->getCode(),
            $throwable
        );
    }

    if($throwable instanceof ConnectionException)
    {
        return new InternalExceptions\ConnectionFailed(
            $throwable->getMessage(),
            (int) $throwable->getCode(),
            $throwable
        );
    }

    return new InternalExceptions\StorageInteractingFailed(
        $throwable->getMessage(),
        (int) $throwable->getCode(),
        $throwable
    );
}

/**
 * @internal
 *
 * @param string $connectionDsn
 *
 * @return AmpPostgreSQLAdapter
 *
 * @throws InternalExceptions\InvalidConfigurationOptions
 */
function postgreSqlAdapterFactory(string $connectionDsn): AmpPostgreSQLAdapter
{
    return new AmpPostgreSQLAdapter(
       new StorageConfiguration($connectionDsn)
    );
}
