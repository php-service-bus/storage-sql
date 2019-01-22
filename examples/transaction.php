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
use function ServiceBus\Storage\Sql\updateQuery;

include __DIR__ . '/../vendor/autoload.php';

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
