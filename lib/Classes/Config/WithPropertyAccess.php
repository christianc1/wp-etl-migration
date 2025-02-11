<?php
/**
 * Trait WithPropertyAccess
 *
 * @package TenupETL\Classes\Config
 */

namespace TenupETL\Classes\Config;

use Symfony\Component\PropertyAccess\PropertyAccess;
use Symfony\Component\PropertyAccess\PropertyAccessor;

trait WithPropertyAccess {
	/**
	 * The Symfony PropertyAccessor instance.
	 *
	 * @var PropertyAccessor
	 */
	protected $accessor;

	/**
	 * Get the PropertyAccessor instance.
	 *
	 * @return PropertyAccessor
	 */
	protected function get_accessor(): PropertyAccessor {
		if ( null === $this->accessor ) {
			$this->accessor = PropertyAccess::createPropertyAccessor();
		}

		return $this->accessor;
	}

	/**
	 * Get a config value
	 *
	 * @param string $property_path The property path.
	 * @param mixed  $default_value The default value to return if not present in the config.
	 * @return mixed
	 */
	public function get_value( string $property_path, mixed $default_value = null ): mixed {
		if ( strpos( $property_path, '[' ) !== 0 ) {
			$parts = explode( '.', $property_path );
			$property_path = '[' . implode( '][', $parts ) . ']';
		}

		return $this->get_accessor()->getValue( $this->get_config(), $property_path, $default_value );
	}
}
