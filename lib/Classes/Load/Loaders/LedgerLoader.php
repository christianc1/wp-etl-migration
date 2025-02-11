<?php
/**
 * Class: WordPress Ledger Loader
 *
 * Reads rows and records a ledger based on any row prefixed with "ledger.".
 *
 * @package TenupETL\Classes\Load\Loaders
 */

namespace TenupETL\Classes\Load\Loaders;

use TenupETL\Utils\{ WithLogging };

use Flow\ETL\{FlowContext, Loader, Rows};

/**
 * Class LedgerLoader
 *
 * Handles loading posts into WordPress during ETL process.
 */
class LedgerLoader extends BaseLoader implements Loader {
	use WithLogging;
	use WithLedger;

	/**
	 * Run the loader
	 *
	 * @param object $state The ETL state object.
	 * @return void
	 */
	public function run( $state ) {
		$state
			->write(
				$this
			);

		$this->log( 'Loading into ledger.', 'progress' );
	}

	/**
	 * Load rows into WordPress as posts
	 *
	 * @param Rows        $rows    The rows to load.
	 * @param FlowContext $context The flow context.
	 * @return void
	 */
	public function load( Rows $rows, FlowContext $context ): void {
		foreach ( $rows as $row ) {
			$ledger    = $this->reduce_row_on_prefix( $row, 'ledger' );
			// Create a ledger entry
			$this->create_ledger_entry(
				array_merge(
					[
						'uid'     => $row->valueOf( 'etl.uid' ),
					],
					$ledger
				)
			);
		}
	}
}
