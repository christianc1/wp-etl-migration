<?php
/**
 * WithLogging Trait
 *
 * Provides logging functionality for classes that need to output messages to WP-CLI.
 *
 * @package TenupETL\Utils
 */

namespace TenupETL\Utils;

trait WithLogging {
	/**
	 * Logs a message with a specified type.
	 *
	 * @param string $message The message to log.
	 * @param string $type The type of log message (info, success, warning, error).
	 */
	protected function log( $message, $type = null ) {
		// Skip progress messages if progress display is disabled
		if ( $type === 'progress' && ! apply_filters( 'tenup_etl_show_progress', true ) ) {
			return;
		}

		if ( is_array( $message ) || is_object( $message ) ) {
			$message = (array) $message;
			// Formats the array so it's ready to print.
			$message = wp_json_encode( $message, JSON_PRETTY_PRINT );
		}

		switch ( $type ) {
			case 'success':
				\WP_CLI::success( $message );
				break;
			case 'warning':
				\WP_CLI::warning( $message );
				break;
			case 'error':
				\WP_CLI::error( $message );
				break;
			case 'debug':
				\WP_CLI::debug( $message );
				break;
			case 'progress':
				\WP_CLI::log( \WP_CLI::colorize( '%CProgress: %n' . $message ) );
				break;
			default:
				$type = $type ? $type : 'Info:';
				\WP_CLI::log( \WP_CLI::colorize( '%C' . $type . '%n' . PHP_EOL . $message ) );
				break;
		}
	}

	/**
	 * Logs a backtrace of the current call stack.
	 *
	 * Useful for debugging by showing the sequence of function calls that led to the current point.
	 */
	protected function log_backtrace() {
		// phpcs:ignore WordPress.PHP.DevelopmentFunctions.error_log_debug_backtrace
		$backtrace = debug_backtrace( DEBUG_BACKTRACE_IGNORE_ARGS );
		$lines     = [ 'Call stack:' ];
		foreach ( $backtrace as $trace ) {
			array_push(
				$lines,
				sprintf(
					'%s::%s (%s:%d)',
					$trace['class'] ?? '',
					$trace['function'] ?? '',
					$trace['file'] ?? '',
					$trace['line'] ?? 0
				)
			);
		}

		$this->log( implode( PHP_EOL, $lines ) );
	}
}
