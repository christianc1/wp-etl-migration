<?php
/**
 * Class: JobConfig
 *
 * @package TenupETL\Classes\Config
 */

namespace TenupETL\Classes\Config;

/**
 * Class JobConfig
 *
 * This class reads a YAML configuration file and returns the configuration as an array.
 *
 * @package TenupETL\Classes\Config
 */
class JobConfig {
	use WithPropertyAccess;

	/**
	 * The configuration as an array.
	 *
	 * @var array
	 */
	protected $config;

	/**
	 * The contructor for the JobConfig class.
	 *
	 * @param array $config The configuration as an array.
	 */
	public function __construct( array $config ) {
		$this->config = $config;
	}

	/**
	 * Get the configuration as an array.
	 *
	 * @return array
	 */
	public function get_config(): array {
		return $this->config;
	}
}
