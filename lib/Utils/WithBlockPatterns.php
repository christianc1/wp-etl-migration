<?php
/**
 * Trait: WithBlockPatterns
 *
 * Provides utility methods for interacting with WordPress Block Patterns.
 *
 * @package TenupETL\Utils
 */

namespace TenupETL\Utils;

/**
 * Trait WithBlockPatterns
 *
 * Utility functions for block pattern handling.
 */
trait WithBlockPatterns {

	/**
	 * Retrieves the content of a registered block pattern.
	 *
	 * Checks if the pattern registry exists and if the specified pattern
	 * is registered and has content.
	 *
	 * @param string $pattern_name The full name of the block pattern (e.g., 'namespace/pattern-slug').
	 * @return string The pattern content (trimmed) or an empty string if not found or invalid.
	 */
	protected function get_pattern( string $pattern_name ): string {
		// Ensure the registry class exists
		if ( ! class_exists( '\\WP_Block_Patterns_Registry' ) ) {
			// Log error or return empty content if registry is not available
			// Maybe add logging here if WithLogging trait is also used.
			return '';
		}

		$registry = \WP_Block_Patterns_Registry::get_instance();
		$registered_pattern = $registry->get_registered( $pattern_name );

		if ( ! $registered_pattern || empty( $registered_pattern['content'] ) ) {
			if ( method_exists( $this, 'log' ) ) {
				$this->log( 'Block pattern included in transformation but it could not be found: ' . $pattern_name, 'warning' );
			}
			return '';
		}

		// Return the registered pattern content directly
		return trim( $registered_pattern['content'] );
	}
}
