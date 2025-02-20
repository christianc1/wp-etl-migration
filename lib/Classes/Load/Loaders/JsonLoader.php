<?php
/**
 * Class: JSON Loader
 *
 * Handles loading data into JSON files during ETL process.
 * Extends BaseLoader to provide JSON-specific loading functionality.
 *
 * @package TenupETL\Classes\Load
 */

namespace TenupETL\Classes\Load\Loaders;

use TenupETL\Utils\WithLogging;

use function TenupETL\Classes\Transform\Transformers\select_prefix;
use function Flow\ETL\Adapter\JSON\{to_json};

/**
 * Class JsonLoader
 *
 * Implements JSON file loading functionality for the ETL process.
 * Handles writing data to JSON files with configurable options and error handling.
 */
class JsonLoader extends BaseLoader {
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
				basename( $this->step_config['destination']['file'], '.json' ) . '-' . $this->uid . '.json',
			]
		);

		$flags = JSON_THROW_ON_ERROR;
		if ( $this->step_config['options']['flags'] ) {
			foreach ( $this->step_config['options']['flags'] as $flag ) {
				$flags |= constant( $flag );
			}
		}

		if ( $this->step_config['prefix'] ) {
			$state = $state->transform( select_prefix( $this->step_config['prefix'], true ) );
		}

		try {
			$state
				->write(
					to_json(
						$destination,
					)->withFlags( $flags )
				);

			$this->log( 'Loading JSON to ' . $destination, 'progress' );
		} catch ( \Exception $e ) {
			$this->log( 'Error loading JSON to ' . $destination . ': ' . $e->getMessage(), 'error' );
		}
	}
}
