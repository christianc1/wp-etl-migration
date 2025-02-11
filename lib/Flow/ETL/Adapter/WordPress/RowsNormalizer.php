<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress;

use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\ETL\Adapter\WordPress\RowsNormalizer\EntryNormalizer;

final class RowsNormalizer
{
    public function __construct(
        private readonly EntryNormalizer $normalizer
    ) {
    }

    public function normalize(Rows $rows): array
    {
        $normalized = [];

        foreach ($rows as $row) {
            $normalizedRow = [];
            foreach ($row->entries() as $entry) {
                $normalizedRow[$entry->name()] = $this->normalizer->normalize($entry);
            }
            $normalized[] = $normalizedRow;
        }

        return $normalized;
    }
}
