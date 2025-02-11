<?php
/**
 * Class CommandRegistrar
 *
 * Handles registration of ETL CLI commands.
 *
 * @package TenupETL
 */

namespace TenupETL;

use TenupETL\Commands\{ConfigCommand, ProcessCommand, JanitorCommand, ExtractCommand, TransformCommand};

/**
 * Class CommandRegistrar
 */
class CommandRegistrar {

	/**
	 * Register all ETL commands
	 *
	 * @param string $namespace Optional namespace to prefix commands with
	 * @return void
	 */
	public static function register( string $namespace = '' ): void {
		if ( ! class_exists( 'WP_CLI' ) ) {
			return;
		}

		$prefix = $namespace ? trim( $namespace ) . ' ' : '';

		\WP_CLI::add_command( $prefix . 'etl config', ConfigCommand::class );
		\WP_CLI::add_command( $prefix . 'etl process', ProcessCommand::class );
		\WP_CLI::add_command( $prefix . 'etl janitor', JanitorCommand::class );
		\WP_CLI::add_command( $prefix . 'etl extract', ExtractCommand::class );
		\WP_CLI::add_command( $prefix . 'etl transform', TransformCommand::class );
	}
}
