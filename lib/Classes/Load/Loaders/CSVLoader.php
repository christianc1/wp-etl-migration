<?php
/**
 * Class: CSV Loader
 *
 * Handles loading data into CSV files during ETL process.
 * Extends BaseLoader to provide CSV-specific loading functionality.
 *
 * @package TenupETL\Classes\Load
 */

namespace TenupETL\Classes\Load\Loaders;

use TenupETL\Utils\WithLogging;

use function TenupETL\Classes\Transform\Transformers\select_prefix;
use function Flow\ETL\Adapter\CSV\{to_csv};

/**
 * Class CSVLoader
 *
 * Implements CSV file loading functionality for the ETL process.
 * Handles writing data to CSV files with configurable options and error handling.
 */
class CSVLoader extends BaseLoader {
	use WithLogging;

	/**
	 * Name of the loader
	 *
	 * @var string
	 */
	protected $name;

	/**
	 * Run the loader
	 *
	 * Writes the ETL state data to a JSON file at the configured destination.
	 * Handles JSON encoding options and error logging.
	 *
	 * @param object $state The ETL state object to write.
	 * @return void
	 */
	public function run( $state ) {
		$destination = join(
			DIRECTORY_SEPARATOR,
			[
				untrailingslashit( TENUP_ETL_PLUGIN_DIR ),
				untrailingslashit( ltrim( $this->step_config['destination']['path'], './' ) ),
				basename( $this->step_config['destination']['file'], '.csv' ) . '-' . $this->uid . '.csv',
			]
		);
		try {
			if ( $this->step_config['prefix'] ) {
				$state = $state->transform( select_prefix( $this->step_config['prefix'], true ) );
			}

			$state
				->write(
					to_csv(
						$destination
					)
				);

			$this->log( 'Loading CSV to ' . $destination, 'progress' );
		} catch ( \Exception $e ) {
			$this->log( 'Error loading CSV to ' . $destination . ': ' . $e->getMessage(), 'error' );
		}
	}
}
