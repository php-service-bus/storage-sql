<?php

/**
 * PHP Service Bus SQL storage implementation
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Storage\Sql\Tests;

use Amp\Promise;
use function Amp\Promise\wait;
use PHPUnit\Framework\TestCase;
use ServiceBus\Storage\Common\DatabaseAdapter;
use function ServiceBus\Storage\Sql\fetchAll;
use function ServiceBus\Storage\Sql\fetchOne;

/**
 *
 */
abstract class BaseStorageAdapterTest extends TestCase
{
    /**
     * Get database adapter
     *
     * @return DatabaseAdapter
     */
    abstract protected static function getAdapter(): DatabaseAdapter;

    /**
     * @inheritdoc
     *
     * @throws \Throwable
     */
    public static function setUpBeforeClass(): void
    {
        parent::setUpBeforeClass();

        $adapter = static::getAdapter();

        wait(
            $adapter->execute(
                'CREATE TABLE IF NOT EXISTS storage_test_table (id UUID, identifier_class VARCHAR NOT NULL, payload BYTEA null , CONSTRAINT identifier PRIMARY KEY (id, identifier_class))'
            )
        );
    }

    /**
     * @inheritdoc
     *
     * @throws \Throwable
     */
    protected function tearDown(): void
    {
        parent::tearDown();

        $adapter = static::getAdapter();

        wait($adapter->execute('DELETE FROM storage_test_table'));
    }

    /**
     * @test
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function unescapeBinary(): void
    {
        $adapter = static::getAdapter();

        $data = \sha1(\random_bytes(256));

        $promise = $adapter->execute(
            'INSERT INTO storage_test_table (id, identifier_class, payload) VALUES (?, ?, ?), (?, ?, ?)',
            [
                '77961031-fd0f-4946-b439-dfc2902b961a', 'SomeIdentifierClass', $data,
                '81c3f1d1-1f75-478e-8bc6-2bb02cd381be', 'SomeIdentifierClass2', \sha1(\random_bytes(256))
            ]
        );

        wait($promise);

        /** @var \ServiceBus\Storage\Common\ResultSet $iterator */
        $iterator = wait($adapter->execute('SELECT * from storage_test_table WHERE id = ?', ['77961031-fd0f-4946-b439-dfc2902b961a']));
        $result   = wait(fetchAll($iterator));

        /** @noinspection StaticInvocationViaThisInspection */
        static::assertCount(1, $result);
        static::assertEquals($data, $adapter->unescapeBinary($result[0]['payload']));
    }

    /**
     * @test
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function resultSet(): void
    {
        $adapter = static::getAdapter();

        wait(self::importFixtures($adapter));

        /** @var \ServiceBus\Storage\Common\ResultSet $iterator */
        $iterator = wait($adapter->execute('SELECT * from storage_test_table'));
        $result   = wait(fetchAll($iterator));

        /** @noinspection StaticInvocationViaThisInspection */
        static::assertCount(2, $result);
    }

    /**
     * @test
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function emptyResultSet(): void
    {
        $adapter = static::getAdapter();

        $iterator = wait($adapter->execute('SELECT * from storage_test_table'));
        $result   = wait(fetchAll($iterator));

        /** @noinspection StaticInvocationViaThisInspection */
        static::assertEmpty($result);
    }

    /**
     * @test
     * @expectedException \ServiceBus\Storage\Common\Exceptions\StorageInteractingFailed
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function failedQuery(): void
    {
        wait(static::getAdapter()->execute('SELECT abube from storage_test_table'));
    }

    /**
     * @test
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function findOne(): void
    {
        $adapter = static::getAdapter();

        wait(self::importFixtures($adapter));

        /** @var \ServiceBus\Storage\Common\ResultSet $iterator */
        $iterator = wait(
            $adapter->execute(
                'SELECT * from storage_test_table WHERE identifier_class = ?',
                ['SomeIdentifierClass2']
            )
        );

        $result = wait(fetchOne($iterator));

        /** @noinspection StaticInvocationViaThisInspection */
        static::assertArrayHasKey('identifier_class', $result);

        /** @noinspection StaticInvocationViaThisInspection */
        static::assertEquals('SomeIdentifierClass2', $result['identifier_class']);
    }

    /**
     * @test
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function findOneWhenEmptySet(): void
    {
        $adapter = static::getAdapter();

        /** @var \ServiceBus\Storage\Common\ResultSet $iterator */
        $iterator = wait(
            $adapter->execute(
                'SELECT * from storage_test_table WHERE identifier_class = ?',
                ['SomeIdentifierClass2']
            )
        );

        $result = wait(fetchOne($iterator));

        /** @noinspection StaticInvocationViaThisInspection */
        static::assertEmpty($result);
    }

    /**
     * @test
     * @expectedException \ServiceBus\Storage\Common\Exceptions\OneResultExpected
     * @expectedExceptionMessage A single record was requested, but the result of the query execution contains several
     *                           ("2")
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function findOneWhenWrongSet(): void
    {
        $adapter = static::getAdapter();

        wait(self::importFixtures($adapter));

        /** @var \ServiceBus\Storage\Common\ResultSet $iterator */
        $iterator = wait($adapter->execute('SELECT * from storage_test_table'));

        wait(fetchOne($iterator));
    }

    /**
     * @test
     * @expectedException \ServiceBus\Storage\Common\Exceptions\UniqueConstraintViolationCheckFailed
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function uniqueKeyCheckFailed(): void
    {
        $adapter = static::getAdapter();

        $promise = $adapter->execute(
            'INSERT INTO storage_test_table (id, identifier_class) VALUES (?, ?), (?, ?)',
            [
                '77961031-fd0f-4946-b439-dfc2902b961a', 'SomeIdentifierClass',
                '77961031-fd0f-4946-b439-dfc2902b961a', 'SomeIdentifierClass'
            ]
        );

        wait($promise);
    }

    /**
     * @test
     *
     * @return void
     *
     * @throws \Throwable
     */
    public function rowsCount(): void
    {
        $adapter = static::getAdapter();

        /** @var \ServiceBus\Storage\Common\ResultSet $result */
        $result = wait(
            $adapter->execute(
                'INSERT INTO storage_test_table (id, identifier_class) VALUES (?, ?), (?, ?)',
                [
                    '77961031-fd0f-4946-b439-dfc2902b961a', 'SomeIdentifierClass',
                    '77961031-fd0f-4946-b439-dfc2902b961d', 'SomeIdentifierClass'
                ]
            )
        );

        static::assertSame(2, $result->affectedRows());

        unset($result);

        /** @var \ServiceBus\Storage\Common\ResultSet $result */
        $result = wait(
            $adapter->execute(
                'DELETE FROM storage_test_table where id = \'77961031-fd0f-4946-b439-dfc2902b961d\''
            )
        );

        static::assertSame(1, $result->affectedRows());

        unset($result);

        wait($adapter->execute('DELETE FROM storage_test_table'));

        /** @var \ServiceBus\Storage\Common\ResultSet $result */
        $result = wait($adapter->execute('DELETE FROM storage_test_table'));

        static::assertSame(0, $result->affectedRows());

        unset($result);

        /** @var \ServiceBus\Storage\Common\ResultSet $result */
        $result = wait(
            $adapter->execute(
                'SELECT * FROM storage_test_table where id = \'77961031-fd0f-4946-b439-dfc2902b961d\''
            )
        );

        static::assertSame(0, $result->affectedRows());

        unset($result);
    }

    /**
     * @param DatabaseAdapter $adapter
     *
     * @return Promise
     *
     * @throws \Throwable
     */
    private static function importFixtures(DatabaseAdapter $adapter): Promise
    {
        return $adapter->execute(
            'INSERT INTO storage_test_table (id, identifier_class) VALUES (?, ?), (?, ?)',
            [
                '77961031-fd0f-4946-b439-dfc2902b961a', 'SomeIdentifierClass',
                '81c3f1d1-1f75-478e-8bc6-2bb02cd381be', 'SomeIdentifierClass2'
            ]
        );
    }
}
