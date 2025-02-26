<?php
/**
 * RowMutator interface
 *
 * @package TenupETL\Classes\Load\Loaders
 */

namespace TenupETL\Classes\Load\Loaders;

use Flow\ETL\Row;

interface RowMutator {
	/**
	 * Mutate a row
	 *
	 * @param Row $row The row to mutate
	 * @return Row The mutated row
	 */
	public function mutate_row( Row $row ): Row;
}
