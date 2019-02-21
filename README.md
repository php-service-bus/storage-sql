## What is it?
[![Build Status](https://travis-ci.org/php-service-bus/storage-sql.svg?branch=v3.0)](https://travis-ci.org/php-service-bus/storage-sql)
[![Code Coverage](https://scrutinizer-ci.com/g/php-service-bus/storage-sql/badges/coverage.png?b=v3.0)](https://scrutinizer-ci.com/g/php-service-bus/storage-sql/?branch=v3.0)
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/php-service-bus/storage-sql/badges/quality-score.png?b=v3.0)](https://scrutinizer-ci.com/g/php-service-bus/storage-sql/?branch=v3.0)

This component is part of the [PHP Service Bus](https://github.com/php-service-bus/service-bus). Implementing PostgreSQL and SQLite adapters.

#### [PostgreSQL](https://github.com/php-service-bus/storage-sql/blob/v3.0/src/AmpPosgreSQL/AmpPostgreSQLAdapter.php)

Non-blocking adapter that supports connection pooling. Implemented based on [Async Postgres client](https://github.com/amphp/postgres).

#### [DoctrineDBAL](https://github.com/php-service-bus/storage-sql/blob/v3.0/src/DoctrineDBAL/DoctrineDBALAdapter.php)
This adapter only supports in memory SQLite and is intended solely **for testing only** (It is blocking and, if used, breaks the operation of the event loop)

## Helpers

In addition to adapters, some helpers are also implemented:

* [fetchOne()](https://github.com/php-service-bus/storage-sql/blob/v3.0/src/functions.php#L76): Transform [ResultSet](https://github.com/php-service-bus/storage-common/blob/8186eaee7a53423a8cc04c954c3ae87e54451c1c/src/ResultSet.php#L20-L60) iterator to array (only 1 item)
* [fetchAll()](https://github.com/php-service-bus/storage-sql/blob/v3.0/src/functions.php#L43): Transform [ResultSet](https://github.com/php-service-bus/storage-common/blob/8186eaee7a53423a8cc04c954c3ae87e54451c1c/src/ResultSet.php#L20-L60) iterator to array (Not recommended for use on large amounts of data)
* [selectQuery()](https://github.com/php-service-bus/storage-sql/blob/v3.0/src/functions.php#L165): Create SELECT query builder
* [updateQuery()](https://github.com/php-service-bus/storage-sql/blob/v3.0/src/functions.php#L178): Create UPDATE query builder
* [deleteQuery()](https://github.com/php-service-bus/storage-sql/blob/v3.0/src/functions.php#L190): Create DELETE query builder
* [insertQuery()](https://github.com/php-service-bus/storage-sql/blob/v3.0/src/functions.php#L205): Create INSERT query builder
* [equalsCriteria()](https://github.com/php-service-bus/storage-sql/blob/v3.0/src/functions.php#L115): Create equals condition
* [notEqualsCriteria()](https://github.com/php-service-bus/storage-sql/blob/v3.0/src/functions.php#L133): Create not equals condition

## Examples

#### Transaction

```php
$adapter = new AmpPostgreSQLAdapter(
   new StorageConfiguration('pgsql://postgres:123456789@localhost:5432/test')
);

Loop::run(
    static function() use ($adapter): \Generator
    {
        $firstUpdateQuery  = updateQuery('some_table', ['key' => 'value'])->compile();
        $secondUpdateQuery = updateQuery('some_another_table', ['key2' => 'value2'])->compile();

        /** @var \ServiceBus\Storage\Common\Transaction $transaction */
        $transaction = yield $adapter->transaction();

        try
        {
            yield $transaction->execute($firstUpdateQuery->sql(), $firstUpdateQuery->params());
            yield $transaction->execute($secondUpdateQuery->sql(), $secondUpdateQuery->params());

            yield $transaction->commit();
        }
        catch(\Throwable $throwable)
        {
            yield $transaction->rollback();
        }
    }
);
```

#### Simple query

```php
$adapter = new AmpPostgreSQLAdapter(
    new StorageConfiguration('pgsql://postgres:123456789@localhost:5432/test')
);

Loop::run(
    static function() use ($adapter): \Generator
    {
        $listEntriesQuery = selectQuery('companies')->compile();

        /** @var \ServiceBus\Storage\Common\ResultSet $resultSet */
        $resultSet = yield $adapter->execute($listEntriesQuery->sql(), $listEntriesQuery->params());

        /** @var array $collection */
        $collection = yield fetchAll($resultSet);

        print_r($collection);
    }
);
```
## Support
* [Telegram chat (RU)](https://t.me/php_service_bus)
* Create issue [https://github.com/php-service-bus/service-bus/issues](https://github.com/php-service-bus/service-bus/issues)

## Contacts
* [`dev@async-php.com`](mailto:dev@async-php.com)

## Security

If you discover any security related issues, please email [`dev@async-php.com`](mailto:dev@async-php.com) instead of using the issue tracker.

## License

The MIT License (MIT). Please see [LICENSE](LICENSE.md) for more information.
