<?php
/**
 * Class GlobalConfig
 *
 * This class reads a YAML configuration file and returns the configuration as an array.
 * It handles importing and merging of multiple YAML files while preventing circular imports.
 *
 * @package TenupETL\Classes\Config
 */

namespace TenupETL\Classes\Config;

use Symfony\Component\Config\Loader\FileLoader;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\Yaml\Yaml;
use Symfony\Component\PropertyAccess\PropertyAccess;
use TenupETL\Classes\Config\YamlWithIncludes;

/**
 * Class: GlobalConfig
 *
 * This class reads a YAML configuration file and returns the configuration as an array.
 *
 * @package TenupETL\Classes\Config
 */
class GlobalConfig extends FileLoader {
	use WithPropertyAccess;

	/**
	 * The configuration as an array.
	 *
	 * @var array
	 */
	protected $config;

	/**
	 * The entrypoint for the configuration.
	 *
	 * @var string
	 */
	protected $entrypoint;

	/**
	 * Tracks processed files to prevent circular imports.
	 *
	 * @var array
	 */
	protected $processed_files = [];

	/**
	 * Constructor.
	 *
	 * @param string $entrypoint The path to the main configuration file.
	 */
	public function __construct( string $entrypoint ) {
		$this->entrypoint = $entrypoint;
		$this->config     = [];
		$this->locator    = new FileLocator( dirname( $entrypoint ) );
		$this->load( $this->entrypoint );
	}

	/**
	 * Gets the configuration entrypoint.
	 *
	 * @return string The entrypoint path.
	 */
	protected function get_entrypoint(): string {
		return $this->entrypoint;
	}

	/**
	 * Sets the configuration entrypoint.
	 *
	 * @param string $entrypoint The new entrypoint path.
	 * @return GlobalConfig The current instance.
	 */
	protected function set_entrypoint( string $entrypoint ): GlobalConfig {
		$this->entrypoint = $entrypoint;

		return $this;
	}

	/**
	 * Loads and parses a YAML configuration file.
	 *
	 * @param mixed       $resource The resource to load.
	 * @param string|null $type     The resource type.
	 * @return GlobalConfig The current instance.
	 */
	public function load( mixed $resource, ?string $type = null ): GlobalConfig {
		// Avoid circular imports
		if ( isset( $this->processed_files[ $resource ] ) ) {
			return $this;
		}
		$this->processed_files[ $resource ] = true;

		$file = $this->locator->locate( $resource );

		$yaml   = new YamlWithIncludes( $this->locator );
		$config = $yaml->parse_file( $file );

		if ( $config ) {
			$this->config = array_merge( $this->config, $config );
		}

		return $this;
	}

	/**
	 * Checks if the loader supports the given resource.
	 *
	 * @param mixed       $resource The resource to check.
	 * @param string|null $type     The resource type.
	 * @return bool True if supported, false otherwise.
	 */
	public function supports( mixed $resource, ?string $type = null ): bool {
		return is_string( $resource ) && 'yaml' === pathinfo( $resource, PATHINFO_EXTENSION );
	}

	/**
	 * Gets the loaded configuration.
	 *
	 * @return array The configuration array.
	 */
	public function get_config(): array {
		return $this->config;
	}

	public function update_config( array $config ): GlobalConfig {
		$this->config = array_merge( $this->config, $config );

		return $this;
	}
}
