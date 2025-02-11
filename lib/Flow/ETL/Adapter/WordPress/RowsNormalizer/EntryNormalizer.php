<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress\RowsNormalizer;

use Flow\ETL\Row\Entry;
use Flow\ETL\PHP\Type\Caster;

final class EntryNormalizer
{
    public function __construct(
        private readonly Caster $caster,
        private readonly string $dateTimeFormat = \DateTimeInterface::ATOM,
    ) {
    }

    public function normalize(Entry $entry): string|float|int|bool|array|null
    {
        return match ($entry::class) {
            Entry\DateTimeEntry::class => $entry->value()?->format($this->dateTimeFormat),
            Entry\JsonEntry::class,
            Entry\ArrayEntry::class,
            Entry\ListEntry::class,
            Entry\MapEntry::class,
            Entry\StructureEntry::class => $entry->value(),
            default => $entry->toString(),
        };
    }
}
