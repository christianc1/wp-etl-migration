<?php
/**
 * Class BaseCommand
 *
 * This class extends the WP_CLI_Command and provides common functionality for all commands.
 * It includes a logger for logging messages and a method to stop ongoing processes or clean up resources.
 *
 * @package TenupETL\Commands
 */

namespace TenupETL\Commands;

use WP_CLI_Command;
use TenupETL\Classes\Config\GlobalConfig;
use TenupETL\Utils\WithLogging;
use WP_CLI;

/**
 * BaseCommand class.
 */
class BaseCommand extends WP_CLI_Command {
	use WithLogging;

	/**
	 * Config
	 *
	 * @var array Configuration array.
	 */
	protected $config;

	/**
	 * Config path
	 *
	 * @var string Path to the configuration file.
	 */
	protected $config_path;

	/**
	 * BaseCommand constructor.
	 * Initializes the logger instance.
	 */
	public function __construct() {
		$this->config_path = TENUP_ETL_PLUGIN_DIR . 'migration.yaml';
		$this->config      = new GlobalConfig( $this->config_path );
	}

	/**
	 * {@inheritDoc}
	 */
	public static function start_bulk_operation() {
		define( 'WP_IMPORTING', true );
		\wp_defer_term_counting( true );
		\wp_defer_comment_counting( true );
		\wp_suspend_cache_invalidation( true );
	}

	/**
	 * {@inheritDoc}
	 */
	public static function end_bulk_operation() {
		\wp_suspend_cache_invalidation( false );
		\wp_cache_flush();

		foreach ( \get_taxonomies() as $tax ) {
			\delete_option( "{$tax}_children" );
			\_get_term_hierarchy( $tax );
		}

		\wp_defer_term_counting( false );
		\wp_defer_comment_counting( false );
	}

	/**
	 * Handles errors that occur during processing.
	 *
	 * @param \Exception $error   The error that occurred.
	 * @param array      $context Additional context about the error.
	 */
	protected function handle_error( $error, $context = [] ) {
		if ( $this->is_fatal_error( $error ) ) {
			$this->rollback();
			WP_CLI::error( 'Fatal error occurred: ' . $error->getMessage() );
		}

		// For non-fatal errors, log and continue
		WP_CLI::warning( 'Error processing item: ' . $error->getMessage() );
		++$this->stats['failed'];
	}

	/**
	 * Determines if an error is fatal and should stop processing.
	 *
	 * @param \Exception $error The error to check.
	 * @return bool Whether the error is fatal.
	 */
	protected function is_fatal_error( $error ) {
		return $error instanceof FatalMigrationException;
	}
}
