<?php
/**
 * Custom YAML parser with include support
 *
 * @package TenupETL\Classes\Config
 */

namespace TenupETL\Classes\Config;

use Symfony\Component\Yaml\Yaml;
use Symfony\Component\Yaml\Parser;
use Symfony\Component\Yaml\Tag\TaggedValue;

/**
 * Extends YAML parsing functionality to support !include tags
 */
class YamlWithIncludes {
	/**
	 * FileLocator instance
	 *
	 * @var \Symfony\Component\Config\FileLocator
	 */
	protected $locator;

	/**
	 * Constructor
	 *
	 * @param \Symfony\Component\Config\FileLocator $locator File locator instance.
	 */
	public function __construct( $locator ) {
		$this->locator = $locator;
	}

	/**
	 * Parses a YAML file into a PHP value.
	 *
	 * @param string $filename The path to the YAML file to be parsed.
	 * @param int    $flags    A bit field of Yaml::PARSE_* constants to customize the YAML parser behavior.
	 * @return mixed The YAML converted to a PHP value
	 */
	public function parse_file( string $filename, int $flags = 0 ) {
		$value = Yaml::parseFile( $filename, Yaml::PARSE_CUSTOM_TAGS | $flags );
		return $this->process_data( $value );
	}

	/**
	 * Recursively process data to handle includes
	 *
	 * @param mixed $data Data to process.
	 * @return mixed
	 */
	protected function process_data( $data ) {
		if ( $data instanceof TaggedValue && 'include' === $data->getTag() ) {
			$included_file = $this->locator->locate( $data->getValue() );
			return $this->parse_file( $included_file );
		}

		if ( is_array( $data ) ) {
			foreach ( $data as $key => $value ) {
				$data[ $key ] = $this->process_data( $value );
			}
		}

		return $data;
	}
}
