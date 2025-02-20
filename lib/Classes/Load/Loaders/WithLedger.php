<?php
/**
 * Trait with Ledger
 *
 * Provides ledger functionality for tracking ETL operations.
 *
 * @package TenupETL\Classes\Load
 */

namespace TenupETL\Classes\Load\Loaders;

use TenupETL\Utils\WithLogging;

use Flow\ETL\{FlowContext, Loader, Rows, Row};
use Flow\ETL\Row\Schema;
use function Flow\ETL\DSL\{data_frame, from_array};
use function Flow\ETL\Adapter\JSON\{to_json};

trait WithLedger {
	/**
	 * Stores ledger entries for tracking ETL operations
	 *
	 * @var array
	 */
	protected $ledger;

	/**
	 * The ledger schema
	 *
	 * @var Schema|null
	 */
	protected $ledger_schema = null;

	/**
	 * Creates a new ledger entry
	 *
	 * @param array $ledger_entry The ledger entry data to store
	 * @return void
	 */
	public function create_ledger_entry( $ledger_entry ) {
		if ( null === $this->ledger ) {
			return;
		}

		array_push( $this->ledger, $ledger_entry );
	}

	/**
	 * Gets the current ledger entries
	 *
	 * @return array|null The ledger entries or null if no entries exist
	 */
	public function get_ledger() {
		return $this->ledger;
	}

	/**
	 * Gets the ledger schema
	 *
	 * @return Schema|null The ledger schema or null if no schema exists
	 */
	public function get_ledger_schema() {
		return $this->ledger_schema;
	}

	/**
	 * Sets the ledger schema
	 *
	 * @param Schema $schema The ledger schema to set
	 * @return void
	 */
	public function set_ledger_schema( Schema $schema ) {
		$this->ledger_schema = $schema;
	}

	/**
	 * Writes the ledger entries to a JSON file
	 *
	 * @return void
	 */
	public function write_ledger() {
		if ( null === $this->ledger ) {
			return;
		}

		$destination = join(
			DIRECTORY_SEPARATOR,
			[
				untrailingslashit( TENUP_ETL_PLUGIN_DIR ),
				untrailingslashit( ltrim( $this->step_config['ledger']['path'], './' ) ),
				$this->name . '-ledger-' . $this->uid . '.json',
			]
		);

		data_frame()
			->read( from_array( $this->ledger ) )
			->batchSize( 1 )
			->write(
				to_json(
					$destination,
				)->withFlags( JSON_PRETTY_PRINT, JSON_INVALID_UTF8_SUBSTITUTE )
			)
			->run();
	}
}
