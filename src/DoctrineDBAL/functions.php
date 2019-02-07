<?php

/**
 * SQL databases adapters implementation
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Storage\Sql\DoctrineDBAL;

use Doctrine\DBAL\Exception as DoctrineDBALExceptions;
use ServiceBus\Storage\Common\Exceptions as InternalExceptions;
use ServiceBus\Storage\Common\StorageConfiguration;

/**
 * Convert Doctrine DBAL exceptions
 *
 * @internal
 *
 * @param \Throwable $throwable
 *
 * @return InternalExceptions\ConnectionFailed|InternalExceptions\UniqueConstraintViolationCheckFailed|InternalExceptions\StorageInteractingFailed
 */
function adaptDbalThrowable(\Throwable $throwable): \Exception
{
    $message = \str_replace(\PHP_EOL, '', $throwable->getMessage());

    if($throwable instanceof DoctrineDBALExceptions\ConnectionException)
    {
        return new InternalExceptions\ConnectionFailed($message, (int) $throwable->getCode(), $throwable);
    }

    if($throwable instanceof DoctrineDBALExceptions\UniqueConstraintViolationException)
    {
        return new InternalExceptions\UniqueConstraintViolationCheckFailed($message, (int) $throwable->getCode(), $throwable);
    }

    return new InternalExceptions\StorageInteractingFailed($message, (int) $throwable->getCode(), $throwable);
}

/**
 * @noinspection PhpDocMissingThrowsInspection
 * @internal
 *
 * @return DoctrineDBALAdapter
 */
function inMemoryAdapter(): DoctrineDBALAdapter
{
    /** @noinspection PhpUnhandledExceptionInspection */
    return new DoctrineDBALAdapter(
        new StorageConfiguration('sqlite:///:memory:')
    );
}
