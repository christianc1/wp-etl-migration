<?php
/**
 * WithRowMutation trait
 *
 * @package TenupETL\Classes\Load\Loaders
 */

namespace TenupETL\Classes\Load\Loaders;

use Flow\ETL\{Rows, Row};
use function Flow\ETL\DSL\{string_entry};

trait WithRowMutation {
	public $mutated_rows;

	public $has_mutated_rows = false;

	public function mutate_row( Row $row ): Row {
		// All rows should have a unique ID
		$uid = (string) $row->valueOf( 'etl.uid' );

		if ( ! $uid ) {
			$row = $row->add( string_entry( 'etl.uid', uuid_v4() ) );
		}

		$this->mutated_rows[$uid] = $row;
		$this->has_mutated_rows = true;

		return $row;
	}

	public function collect_mutated_rows(): Rows {
		$rows = new Rows( ...array_values( $this->mutated_rows ) );

		$this->mutated_rows = [];
		$this->has_mutated_rows = false;

		return $rows;
	}

	public function has_mutated_rows(): bool {
		return $this->has_mutated_rows;
	}
}
