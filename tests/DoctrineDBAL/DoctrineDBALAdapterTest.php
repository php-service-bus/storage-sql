<?php

/**
 * SQL databases adapters implementation.
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Storage\Sql\Tests\DoctrineDBAL;

use function Amp\Promise\wait;
use function ServiceBus\Storage\Sql\DoctrineDBAL\inMemoryAdapter;
use ServiceBus\Storage\Common\DatabaseAdapter;
use ServiceBus\Storage\Common\Exceptions\ConnectionFailed;
use ServiceBus\Storage\Common\Exceptions\StorageInteractingFailed;
use ServiceBus\Storage\Common\StorageConfiguration;
use ServiceBus\Storage\Sql\DoctrineDBAL\DoctrineDBALAdapter;
use ServiceBus\Storage\Sql\Tests\BaseStorageAdapterTest;

/**
 *
 */
final class DoctrineDBALAdapterTest extends BaseStorageAdapterTest
{
    /**
     * @var DoctrineDBALAdapter
     */
    private static $adapter;

    /**
     * {@inheritdoc}
     */
    protected static function getAdapter(): DatabaseAdapter
    {
        if (null === self::$adapter)
        {
            self::$adapter = inMemoryAdapter();
        }

        return self::$adapter;
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable
     */
    protected function setUp(): void
    {
        parent::setUp();

        wait(
            static::getAdapter()->execute(
                'CREATE TABLE IF NOT EXISTS test_ai (id serial PRIMARY KEY, value VARCHAR)'
            )
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     *
     */
    public function lastInsertId(): void
    {
        $adapter = static::getAdapter();

        /** @var \ServiceBus\Storage\Common\ResultSet $result */
        $result = wait($adapter->execute('INSERT INTO test_ai (value) VALUES (\'qwerty\')'));

        static::assertSame('1', wait($result->lastInsertId()));

        /** @var \ServiceBus\Storage\Common\ResultSet $result */
        $result = wait($adapter->execute('INSERT INTO test_ai (value) VALUES (\'qwerty\')'));

        static::assertSame('2', wait($result->lastInsertId()));
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     *
     */
    public function failedConnection(): void
    {
        $this->expectException(ConnectionFailed::class);

        $adapter = new DoctrineDBALAdapter(
            new StorageConfiguration('pgsql://localhost:4486/foo?charset=UTF-8')
        );

        wait($adapter->execute('SELECT now()'));
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     *
     */
    public function failedConnectionString(): void
    {
        $this->expectException(StorageInteractingFailed::class);

        $adapter = new DoctrineDBALAdapter(
            new StorageConfiguration('')
        );

        wait($adapter->execute('SELECT now()'));
    }
}
