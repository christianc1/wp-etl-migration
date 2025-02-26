<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\WordPress;

use Flow\ETL\Row\Entry;
use Flow\ETL\Row;
use Flow\ETL\Adapter\WordPress\RowsNormalizer\EntryNormalizer;

final class RowNormalizer
{
    public function __construct(
        private readonly EntryNormalizer $normalizer
    ) {
    }

    public function normalize(Row $row): array
    {

    	$normalizedRow = [];
    	foreach ($row->entries() as $entry) {
    	    $normalizedRow[$entry->name()] = $this->normalizer->normalize($entry);
    	}

        return $normalizedRow;
    }
}
