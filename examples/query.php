<?php

/**
 * PHP Service Bus (publish-subscribe pattern implementation) storage sql implementation
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

use Amp\Loop;
use ServiceBus\Storage\Common\StorageConfiguration;
use ServiceBus\Storage\Sql\AmpPosgreSQL\AmpPostgreSQLAdapter;
use function ServiceBus\Storage\Sql\fetchAll;
use function ServiceBus\Storage\Sql\selectQuery;

include __DIR__ . '/../vendor/autoload.php';

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

        /** @noinspection ForgottenDebugOutputInspection */
        print_r($collection);
    }
);
