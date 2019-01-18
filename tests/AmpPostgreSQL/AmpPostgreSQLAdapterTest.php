<?php

/**
 * PHP Service Bus (publish-subscribe pattern implementation) storage sql implementation
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Storage\Sql\Tests\AmpPostgreSQL;

use function Amp\Promise\wait;
use ServiceBus\Storage\Common\DatabaseAdapter;
use ServiceBus\Storage\Common\StorageConfiguration;
use ServiceBus\Storage\Sql\AmpPosgreSQL\AmpPostgreSQLAdapter;
use function ServiceBus\Storage\Sql\AmpPosgreSQL\postgreSqlAdapterFactory;
use ServiceBus\Storage\Sql\Tests\BaseStorageAdapterTest;

/**
 *
 */
final class AmpPostgreSQLAdapterTest extends BaseStorageAdapterTest
{
    /**
     * @var AmpPostgreSQLAdapter
     */
    private static $adapter;

    /**
     * @return void
     *
     * @throws \Throwable
     */
    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();

        wait(
            static::getAdapter()->execute(
                'CREATE TABLE IF NOT EXISTS test_ai (id serial PRIMARY KEY, value VARCHAR)'
            )
        );
    }

    /**
     * @return void
     *
     * @throws \Throwable
     */
    public static function tearDownAfterClass(): void
    {
        $adapter = static::getAdapter();

        wait($adapter->execute('DROP TABLE storage_test_table'));
        wait($adapter->execute('DROP TABLE test_ai'));

        self::$adapter = null;
    }

    /**
     * @inheritdoc
     *
     * @throws \Throwable
     */
    protected static function getAdapter(): DatabaseAdapter
    {
        if(null === self::$adapter)
        {
            self::$adapter = postgreSqlAdapterFactory((string) \getenv('TEST_POSTGRES_DSN'));
        }

        return self::$adapter;
    }

    /**
     * @test
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function lastInsertId(): void
    {
        $adapter = static::getAdapter();

        /** @var \ServiceBus\Storage\Common\ResultSet $result */
        $result = wait($adapter->execute('INSERT INTO test_ai (value) VALUES (\'qwerty\') RETURNING id'));

        static::assertEquals('1', $result->lastInsertId());

        /** @var \ServiceBus\Storage\Common\ResultSet $result */
        $result = wait($adapter->execute('INSERT INTO test_ai (value) VALUES (\'qwerty\') RETURNING id'));

        static::assertEquals('2', $result->lastInsertId());
    }

    /**
     * @test
     * @expectedException \ServiceBus\Storage\Common\Exceptions\ConnectionFailed
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function failedConnection(): void
    {
        $adapter = new AmpPostgreSQLAdapter(
            StorageConfiguration::fromDSN('qwerty')
        );

        wait($adapter->execute('SELECT now()'));
    }
}
